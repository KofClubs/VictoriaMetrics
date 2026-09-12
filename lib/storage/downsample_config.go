package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// DownsamplingConfig 保存基础分辨率和租户附加分辨率。安装到 Storage 后作为不可变快照使用。
// 公开访问方法返回副本；包内作业可只读借用切片，不能修改已安装的配置。
type DownsamplingConfig struct {
	// baseResolutionMs 是固定的基础分辨率，单位为毫秒；存储目录首次启用后不可更改。
	baseResolutionMs int64
	// baseResolutions 只包含基础分辨率，供未配置附加分辨率的租户只读借用。
	baseResolutions []int64
	// tenantResolutions 以完整租户标识为键；每个有附加配置的租户保存 BASE 与升序附加分辨率。
	// 切片由本配置拥有；包内只读借用，公开访问时复制。
	tenantResolutions map[TenantToken][]int64
	// maxResolutionsPerTenant 是单个租户最多输出的分辨率数，包含 BASE，用于保守估算磁盘输出。
	// 它不是所有租户分辨率并集的大小，不能用于估算全部 spill 的内存峰值。
	maxResolutionsPerTenant int
}

// downsamplingConfigJSON 是启动参数、运行时 API 和 part metadata 共用的 JSON 表示。
type downsamplingConfigJSON struct {
	// BaseResolution 使用 duration 字符串表示不可变的基础分辨率。
	BaseResolution string `json:"base_resolution"`
	// TenantResolutions 仅列出具有附加分辨率的租户，不重复隐含的 BASE。
	TenantResolutions []downsamplingTenantConfigJSON `json:"tenant_resolutions"`
}

type downsamplingTenantConfigJSON struct {
	// Tenant 使用规范的十进制 accountID:projectID，两个标识均在 uint32 范围内。
	Tenant string `json:"tenant"`
	// Resolutions 只包含大于 BASE 的整数倍；解析时去除排序差异并拒绝重复值。
	Resolutions []string `json:"resolutions"`
}

var errDownsamplingBaseChanged = errors.New("[downsampling] base_resolution cannot be changed")

const downsamplingBaseFilename = "downsampling.json"

// ParseDownsamplingConfig 严格校验完整 JSON，构造拥有独立切片和映射的配置。
func ParseDownsamplingConfig(data []byte) (*DownsamplingConfig, error) {
	if len(data) == 0 || len(data) > downsampleMaxMetadataSize {
		return nil, fmt.Errorf("[downsampling] configuration must contain 1..%d bytes", downsampleMaxMetadataSize)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := validateDownsamplingJSONValue(decoder); err != nil {
		return nil, fmt.Errorf("[downsampling] invalid configuration JSON: %w", err)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("[downsampling] unexpected trailing configuration data")
	}
	var wire downsamplingConfigJSON
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&wire); err != nil {
		return nil, fmt.Errorf("[downsampling] invalid configuration: %w", err)
	}
	base, err := parseDownsampleResolution(wire.BaseResolution)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] invalid base_resolution: %w", err)
	}
	c := &DownsamplingConfig{baseResolutionMs: base, baseResolutions: []int64{base}, tenantResolutions: make(map[TenantToken][]int64), maxResolutionsPerTenant: 1}
	seen := make(map[TenantToken]bool)
	for _, entry := range wire.TenantResolutions {
		tenant, err := parseDownsamplingTenant(entry.Tenant)
		if err != nil {
			return nil, err
		}
		if seen[tenant] {
			return nil, fmt.Errorf("[downsampling] duplicate tenant %q", entry.Tenant)
		}
		seen[tenant] = true
		if entry.Resolutions == nil {
			return nil, fmt.Errorf("[downsampling] tenant %q is missing resolutions", entry.Tenant)
		}
		resolutions := []int64{base}
		for _, value := range entry.Resolutions {
			resolution, err := parseDownsampleResolution(value)
			if err != nil {
				return nil, fmt.Errorf("[downsampling] invalid resolution for tenant %q: %w", entry.Tenant, err)
			}
			if resolution <= base || resolution%base != 0 {
				return nil, fmt.Errorf("[downsampling] tenant %q resolution %q must be a larger integer multiple of base_resolution", entry.Tenant, value)
			}
			resolutions = append(resolutions, resolution)
		}
		sort.Slice(resolutions, func(i, j int) bool { return resolutions[i] < resolutions[j] })
		for i := 1; i < len(resolutions); i++ {
			if resolutions[i] == resolutions[i-1] {
				return nil, fmt.Errorf("[downsampling] duplicate resolution for tenant %q", entry.Tenant)
			}
		}
		if len(resolutions) > 1 {
			c.tenantResolutions[tenant] = resolutions
			c.maxResolutionsPerTenant = max(c.maxResolutionsPerTenant, len(resolutions))
		}
	}
	// 用实际 metadata 结构预检完整规范化配置的最坏编码长度，每个 part 的租户配置均为其子集。
	// 计数、时间字段采用最大合法长度；不另设租户或分辨率数量上限。
	metadata := newDownsamplePartMetadata(partHeader{
		RowsCount: math.MaxUint64, BlocksCount: math.MaxUint64,
		MinTimestamp: maxUnixMilli, MaxTimestamp: maxUnixMilli,
	}, c)
	encodedMetadata, err := json.Marshal(&metadata)
	if err != nil {
		return nil, fmt.Errorf("[downsampling] cannot encode configuration metadata: %w", err)
	}
	if len(encodedMetadata) > downsampleMaxMetadataSize {
		return nil, fmt.Errorf("[downsampling] configuration requires up to %d metadata bytes; maximum is %d", len(encodedMetadata), downsampleMaxMetadataSize)
	}
	return c, nil
}

// validateDownsamplingJSONValue 拒绝任意层级的重复字段和 null，避免静默覆盖配置。
func validateDownsamplingJSONValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("null is not allowed")
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]bool)
		for d.More() {
			token, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := token.(string)
			if !ok || seen[key] {
				return fmt.Errorf("invalid or duplicate field %q", key)
			}
			seen[key] = true
			if err := validateDownsamplingJSONValue(d); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := validateDownsamplingJSONValue(d); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delim)
	}
	_, err = d.Token()
	return err
}

func parseDownsampleResolution(value string) (int64, error) {
	duration, err := time.ParseDuration(value)
	if err != nil && len(value) > 1 {
		unit := int64(0)
		switch value[len(value)-1] {
		case 'd':
			unit = int64(24 * time.Hour)
		case 'w':
			unit = int64(7 * 24 * time.Hour)
		case 'y':
			unit = int64(365 * 24 * time.Hour)
		}
		if unit != 0 {
			n, parseErr := strconv.ParseUint(value[:len(value)-1], 10, 64)
			if parseErr == nil && n <= uint64(math.MaxInt64/unit) {
				duration, err = time.Duration(n*uint64(unit)), nil
			}
		}
	}
	if err != nil || duration <= 0 || duration%time.Millisecond != 0 || !validDownsampleResolution(duration.Milliseconds()) {
		return 0, fmt.Errorf("[downsampling] resolution %q must be a positive whole number of milliseconds", value)
	}
	return duration.Milliseconds(), nil
}

func parseDownsamplingTenant(value string) (TenantToken, error) {
	parts := strings.Split(value, ":")
	var tenant TenantToken
	if len(parts) != 2 {
		return tenant, fmt.Errorf("[downsampling] invalid tenant %q; expected accountID:projectID", value)
	}
	values := []*uint32{&tenant.AccountID, &tenant.ProjectID}
	for i, part := range parts {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return TenantToken{}, fmt.Errorf("[downsampling] invalid tenant %q; expected canonical uint32 accountID:projectID", value)
		}
		*values[i] = uint32(n)
	}
	return tenant, nil
}

func defaultDownsamplingConfig() *DownsamplingConfig {
	return &DownsamplingConfig{baseResolutionMs: 300000, baseResolutions: []int64{300000}, maxResolutionsPerTenant: 1}
}

// BaseResolutionMs 返回基础分辨率的毫秒数。
func (c *DownsamplingConfig) BaseResolutionMs() int64 { return c.baseResolutionMs }

// MaxResolutionsPerTenant 返回单个租户最多输出的分辨率数，包含 BASE。
func (c *DownsamplingConfig) MaxResolutionsPerTenant() int { return c.maxResolutionsPerTenant }

// ResolutionsForTenant 返回该租户的升序分辨率副本，首项始终为 BASE。
func (c *DownsamplingConfig) ResolutionsForTenant(accountID, projectID uint32) []int64 {
	return append([]int64(nil), c.resolutionsForTenant(accountID, projectID)...)
}

// resolutionsForTenant 返回配置拥有的只读切片；调用方不能修改或保留为可变缓冲。
func (c *DownsamplingConfig) resolutionsForTenant(accountID, projectID uint32) []int64 {
	if r := c.tenantResolutions[TenantToken{AccountID: accountID, ProjectID: projectID}]; len(r) != 0 {
		return r
	}
	return c.baseResolutions
}

// forTenants 复制基础分辨率及指定租户的附加配置，供 part metadata 排除无关租户。
func (c *DownsamplingConfig) forTenants(tenants []TenantToken) *DownsamplingConfig {
	result := &DownsamplingConfig{baseResolutionMs: c.baseResolutionMs, baseResolutions: []int64{c.baseResolutionMs}, tenantResolutions: make(map[TenantToken][]int64), maxResolutionsPerTenant: 1}
	for _, tenant := range tenants {
		if resolutions := c.tenantResolutions[tenant]; len(resolutions) > 1 {
			result.tenantResolutions[tenant] = append([]int64(nil), resolutions...)
			result.maxResolutionsPerTenant = max(result.maxResolutionsPerTenant, len(resolutions))
		}
	}
	return result
}

// clone 深复制配置，使公开返回值和调用方输入均不会引用 Storage 的活动快照。
func (c *DownsamplingConfig) clone() *DownsamplingConfig {
	tenants := make([]TenantToken, 0, len(c.tenantResolutions))
	for tenant := range c.tenantResolutions {
		tenants = append(tenants, tenant)
	}
	return c.forTenants(tenants)
}

// MarshalJSON 按完整租户标识及分辨率排序输出，使用规范 duration 字符串。
func (c *DownsamplingConfig) MarshalJSON() ([]byte, error) {
	if c == nil || c.baseResolutionMs <= 0 {
		return nil, fmt.Errorf("[downsampling] invalid configuration")
	}
	wire := downsamplingConfigJSON{BaseResolution: (time.Duration(c.baseResolutionMs) * time.Millisecond).String(), TenantResolutions: []downsamplingTenantConfigJSON{}}
	tenants := make([]TenantToken, 0, len(c.tenantResolutions))
	for tenant := range c.tenantResolutions {
		tenants = append(tenants, tenant)
	}
	sort.Slice(tenants, func(i, j int) bool {
		return tenants[i].AccountID < tenants[j].AccountID || (tenants[i].AccountID == tenants[j].AccountID && tenants[i].ProjectID < tenants[j].ProjectID)
	})
	for _, tenant := range tenants {
		entry := downsamplingTenantConfigJSON{Tenant: fmt.Sprintf("%d:%d", tenant.AccountID, tenant.ProjectID), Resolutions: []string{}}
		for _, resolution := range c.tenantResolutions[tenant][1:] {
			entry.Resolutions = append(entry.Resolutions, (time.Duration(resolution) * time.Millisecond).String())
		}
		wire.TenantResolutions = append(wire.TenantResolutions, entry)
	}
	return json.Marshal(wire)
}

// UnmarshalJSON 只在完整校验成功后替换接收者；Storage 安装和返回配置时均进行深复制。
func (c *DownsamplingConfig) UnmarshalJSON(data []byte) error {
	parsed, err := ParseDownsamplingConfig(data)
	if err != nil {
		return err
	}
	*c = *parsed
	return nil
}

// GetDownsamplingConfig 返回与活动配置完全分离的副本，包括其映射和分辨率切片。
func (s *Storage) GetDownsamplingConfig() *DownsamplingConfig {
	return s.getDownsamplingConfig().clone()
}

// getDownsamplingConfig 返回一次原子读取的不可变快照，供同一作业始终借用。
func (s *Storage) getDownsamplingConfig() *DownsamplingConfig {
	if c := s.downsamplingConfig.Load(); c != nil {
		return c
	}
	return defaultDownsamplingConfig()
}

// UpdateDownsamplingConfig 原子替换租户附加分辨率，后续作业读取新快照。
// BASE 不可更改；运行时更新不写入磁盘，重启后由启动 JSON 决定附加分辨率。
func (s *Storage) UpdateDownsamplingConfig(c *DownsamplingConfig) error {
	if c == nil || c.baseResolutionMs <= 0 {
		return fmt.Errorf("[downsampling] invalid configuration")
	}
	s.downsamplingConfigLock.Lock()
	defer s.downsamplingConfigLock.Unlock()
	if !s.downsamplingEnabled {
		return fmt.Errorf("[downsampling] storage downsampling is disabled")
	}
	if c.baseResolutionMs != s.getDownsamplingConfig().baseResolutionMs {
		return errDownsamplingBaseChanged
	}
	s.downsamplingConfig.Store(c.clone())
	return nil
}

// checkDownsamplingBase 只读校验已持久化的 BASE；首次启用时不会提前创建文件。
func checkDownsamplingBase(path string, config *DownsamplingConfig) error {
	data, err := readDownsampleLimitedFile(filepath.Join(path, metadataDirname, downsamplingBaseFilename), downsampleMaxMetadataSize)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stored, err := ParseDownsamplingConfig(data)
	if err != nil {
		return err
	}
	if len(stored.tenantResolutions) != 0 {
		return fmt.Errorf("[downsampling] persisted base contains tenant resolutions")
	}
	if config.baseResolutionMs != stored.baseResolutionMs {
		return errDownsamplingBaseChanged
	}
	return nil
}

// mustPersistDownsamplingBase 首次启用时原子保存 BASE；metadata 目录随存储快照复制。
// 已有记录由打开前的只读校验确认，不被运行时租户更新覆盖。
func mustPersistDownsamplingBase(path string, config *DownsamplingConfig) {
	metadataPath := filepath.Join(path, metadataDirname)
	filename := filepath.Join(metadataPath, downsamplingBaseFilename)
	if fs.IsPathExist(filename) {
		return
	}
	data, err := config.forTenants(nil).MarshalJSON()
	if err != nil {
		panic(err)
	}
	fs.MustMkdirIfNotExist(metadataPath)
	fs.MustWriteAtomic(filename, data, false)
}
