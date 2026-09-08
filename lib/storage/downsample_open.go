package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

// checkDownsamplingOpen 在 storage 持有 flock 时检查配置和活动文件，不修改目录或清单。
func checkDownsamplingOpen(path string, opts OpenOptions) error {
	if opts.DownsamplingEnabled && GetDedupInterval() != 0 {
		return fmt.Errorf("-storage.downsampling.enabled requires -dedup.minScrapeInterval=0; got %dms", GetDedupInterval())
	}
	dataPath := filepath.Join(path, dataDirname)
	rootPaths := [3]string{
		filepath.Join(dataPath, smallDirname),
		filepath.Join(dataPath, bigDirname),
		filepath.Join(dataPath, indexdbDirname),
	}
	var roots [3]map[string]bool
	partitionNames := make(map[string]bool)
	for i, rootPath := range rootPaths {
		names, err := readDownsamplePartitionNames(rootPath)
		if err != nil {
			return err
		}
		roots[i] = names
		for name := range names {
			partitionNames[name] = true
		}
	}
	names := make([]string, 0, len(partitionNames))
	for name := range partitionNames {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		paths := [2]string{filepath.Join(rootPaths[0], name), filepath.Join(rootPaths[1], name)}
		active := [2]bool{roots[0][name], roots[1][name]}
		partNames, err := readDownsampleActivePartNames(paths, active)
		if err != nil {
			return err
		}
		for i, names := range partNames {
			for _, partName := range names {
				partPath := filepath.Join(paths[i], partName)
				info, err := os.Stat(partPath)
				if err != nil {
					return fmt.Errorf("cannot inspect active part %q: %w", partPath, err)
				}
				if !info.IsDir() {
					return fmt.Errorf("active part %q is not a directory", partPath)
				}
				downsampled, err := detectDownsampleFormat(partPath)
				if err != nil {
					return fmt.Errorf("cannot inspect active part %q: %w", partPath, err)
				}
				if downsampled && !opts.DownsamplingEnabled {
					return fmt.Errorf("active part %q uses downsampling format %d; enable -storage.downsampling.enabled to open this storage", partPath, downsampleFormatVersion)
				}
			}
		}
	}
	return nil
}

// readDownsamplePartitionNames 沿用 table 的目录识别规则，但不删除未完整清理的 partition。
// IndexDB 仅参与 partition 名称发现，不读取其中的索引文件。
func readDownsamplePartitionNames(path string) (map[string]bool, error) {
	des, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot enumerate partitions at %q: %w", path, err)
	}
	names := make(map[string]bool)
	for _, de := range des {
		if !fs.IsDirOrSymlink(de) || de.Name() == snapshotsDirname {
			continue
		}
		partitionPath := filepath.Join(path, de.Name())
		entries, err := os.ReadDir(partitionPath)
		if err != nil {
			return nil, fmt.Errorf("cannot inspect partition directory %q: %w", partitionPath, err)
		}
		// 与 fs.IsPartiallyRemovedDir 保持一致：空目录和删除标记均不属于活动集合。
		partiallyRemoved := len(entries) == 0
		for _, entry := range entries {
			if !entry.IsDir() && entry.Name() == ".delete-this-dir" {
				partiallyRemoved = true
				break
			}
		}
		if partiallyRemoved {
			continue
		}
		var tr TimeRange
		if err := tr.fromPartitionName(de.Name()); err != nil {
			return nil, fmt.Errorf("invalid partition directory %q: %w", partitionPath, err)
		}
		names[de.Name()] = true
	}
	return names, nil
}

// readDownsampleActivePartNames 优先读取 parts.json；历史目录缺少清单时按原有规则发现 part。
func readDownsampleActivePartNames(paths [2]string, active [2]bool) ([2][]string, error) {
	var names [2][]string
	partsFile := filepath.Join(paths[0], partsFilename)
	var data []byte
	err := os.ErrNotExist
	if active[0] {
		data, err = os.ReadFile(partsFile)
	}
	if err == nil {
		partNames, err := parseDownsamplePartNames(data)
		if err != nil {
			return names, fmt.Errorf("cannot parse active part manifest %q: %w", partsFile, err)
		}
		names = [2][]string{partNames.Small, partNames.Big}
	} else if errors.Is(err, os.ErrNotExist) {
		for i, path := range paths {
			if !active[i] {
				continue
			}
			des, err := os.ReadDir(path)
			if err != nil {
				return names, fmt.Errorf("cannot enumerate historical parts at %q: %w", path, err)
			}
			for _, de := range des {
				if fs.IsDirOrSymlink(de) && !isSpecialDir(de.Name()) {
					names[i] = append(names[i], de.Name())
				}
			}
		}
	} else {
		return names, fmt.Errorf("cannot read active part manifest %q: %w", partsFile, err)
	}
	for i, partNames := range names {
		seen := make(map[string]bool, len(partNames))
		for _, name := range partNames {
			if name == "" || name == "." || name == ".." || strings.ContainsAny(name, `/\`) || isSpecialDir(name) {
				return names, fmt.Errorf("invalid active part name %q in %q", name, partsFile)
			}
			if !active[i] {
				return names, fmt.Errorf("active part %q is listed in %q, but its partition directory is missing or marked for deletion", filepath.Join(paths[i], name), partsFile)
			}
			if seen[name] {
				return names, fmt.Errorf("duplicate active part %q in %q", filepath.Join(paths[i], name), partsFile)
			}
			seen[name] = true
		}
	}
	return names, nil
}

// parseDownsamplePartNames 拒绝重复字段和未知字段，防止损坏清单隐式丢失活动 part。
func parseDownsamplePartNames(data []byte) (partNamesJSON, error) {
	var names partNamesJSON
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil {
		return names, err
	}
	if token != json.Delim('{') {
		return names, fmt.Errorf("expected a JSON object")
	}
	seen := make(map[string]bool, 2)
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return names, err
		}
		key := token.(string)
		canonicalKey := strings.ToLower(key)
		if seen[canonicalKey] {
			return names, fmt.Errorf("duplicate field %q", key)
		}
		seen[canonicalKey] = true
		var dst *[]string
		switch canonicalKey {
		case "small":
			dst = &names.Small
		case "big":
			dst = &names.Big
		default:
			return names, fmt.Errorf("unknown field %q", key)
		}
		if err := d.Decode(dst); err != nil {
			return names, fmt.Errorf("invalid %q list: %w", key, err)
		}
	}
	if _, err := d.Token(); err != nil {
		return names, err
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return names, fmt.Errorf("unexpected trailing manifest data")
	}
	return names, nil
}
