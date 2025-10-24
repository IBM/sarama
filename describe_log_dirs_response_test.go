//go:build !functional

package sarama

import (
	"testing"
)

var (
	describeLogDirsResponseEmpty = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, 0, 0, // no log dirs
	}

	describeLogDirsResponseTwoPartitions = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, 0, 1, // One describe log dir (array length)
		0, 0, // No error code
		0, 6, // Character length of path (6 chars)
		'/', 'k', 'a', 'f', 'k', 'a',
		0, 0, 0, 1, // One DescribeLogDirsResponseTopic (array length)
		0, 6, // Character length of "random" topic (6 chars)
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		0, 0, 0, 2, // Two DescribeLogDirsResponsePartition (array length)
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 125, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,           // IsTemporary = false
		0, 0, 0, 26, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 100, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0, // IsTemporary = false
	}

	describeLogDirsResponseEmptyV2 = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, // no log dirs
	}

	describeLogDirsResponseTwoPartitionsV2 = []byte{
		0, 0, 0, 0, // no throttle time
		2,    // One describe log dir (array length + 1)
		0, 0, // No error code
		7, // Character length of path (6 chars + 1)
		'/', 'k', 'a', 'f', 'k', 'a',
		2,                            // One DescribeLogDirsResponseTopic (array length + 1)
		7,                            // Character length of "random" topic (6 chars + 1)
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		3,           // Two DescribeLogDirsResponsePartition (array length)
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 125, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,           // IsTemporary = false
		0,           // empty tagged fields
		0, 0, 0, 26, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 100, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,    // IsTemporary = false
		0, 0, // empty tagged fields
		0, 0, // empty tagged fields
	}

	describeLogDirsResponseEmptyV3 = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, // No error code
		1, // no log dirs
		0, // empty tagged fields
	}

	describeLogDirsResponseTwoPartitionsV3 = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, // No error code
		2,    // One describe log dir (array length + 1)
		0, 0, // No error code
		7, // Character length of path (6 chars + 1)
		'/', 'k', 'a', 'f', 'k', 'a',
		2,                            // One DescribeLogDirsResponseTopic (array length + 1)
		7,                            // Character length of "random" topic (6 chars + 1)
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		3,           // Two DescribeLogDirsResponsePartition (array length)
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 125, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,           // IsTemporary = false
		0,           // empty tagged fields
		0, 0, 0, 26, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 100, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,    // IsTemporary = false
		0, 0, // empty tagged fields
		0, 0, // empty tagged fields
	}

	describeLogDirsResponseEmptyV4 = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, // No error code
		0, 0, // no log dirs
	}

	describeLogDirsResponseTwoPartitionsV4 = []byte{
		0, 0, 0, 0, // no throttle time
		0, 0, // No error code
		2,    // One describe log dir (array length + 1)
		0, 0, // No error code
		7, // Character length of path (6 chars + 1)
		'/', 'k', 'a', 'f', 'k', 'a',
		2,                            // One DescribeLogDirsResponseTopic (array length + 1)
		7,                            // Character length of "random" topic (6 chars + 1)
		'r', 'a', 'n', 'd', 'o', 'm', // Topic name
		3,           // Two DescribeLogDirsResponsePartition (array length)
		0, 0, 0, 25, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 125, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,           // IsTemporary = false
		0,           // empty tagged fields
		0, 0, 0, 26, // PartitionID 25
		0, 0, 0, 0, 0, 0, 0, 100, // Log Size
		0, 0, 0, 0, 0, 0, 0, 0, // OffsetLag
		0,    // IsTemporary = false
		0, 0, // empty tagged fields
		0, 0, 0, 63, 247, 0, 32, 0,
		0, 0, 0, 18, 189, 95, 128, 0,
		0, // empty tagged fields
		0, // empty tagged fields
	}
)

func TestDescribeLogDirsResponse(t *testing.T) {
	// Test empty response
	response := &DescribeLogDirsResponse{
		LogDirs: []DescribeLogDirsResponseDirMetadata{},
	}
	testVersionDecodable(t, "empty", response, describeLogDirsResponseEmpty, 0)
	if len(response.LogDirs) != 0 {
		t.Error("Expected no log dirs")
	}

	response.LogDirs = []DescribeLogDirsResponseDirMetadata{
		{
			ErrorCode: 0,
			Path:      "/kafka",
			Topics: []DescribeLogDirsResponseTopic{
				{
					Topic: "random",
					Partitions: []DescribeLogDirsResponsePartition{
						{
							PartitionID: 25,
							Size:        125,
							OffsetLag:   0,
							IsTemporary: false,
						},
						{
							PartitionID: 26,
							Size:        100,
							OffsetLag:   0,
							IsTemporary: false,
						},
					},
				},
			},
		},
	}
	testVersionDecodable(t, "two partitions", response, describeLogDirsResponseTwoPartitions, 0)
	if len(response.LogDirs) != 1 {
		t.Error("Expected one log dirs")
	}
	if len(response.LogDirs[0].Topics) != 1 {
		t.Error("Expected one topic in log dirs")
	}
	if len(response.LogDirs[0].Topics[0].Partitions) != 2 {
		t.Error("Expected two partitions")
	}
}

func TestDescribeLogDirsResponseV2(t *testing.T) {
	// Test empty response
	response := &DescribeLogDirsResponse{
		LogDirs: []DescribeLogDirsResponseDirMetadata{},
	}
	testVersionDecodable(t, "empty", response, describeLogDirsResponseEmptyV2, 2)
	if len(response.LogDirs) != 0 {
		t.Error("Expected no log dirs")
	}

	response.LogDirs = []DescribeLogDirsResponseDirMetadata{
		{
			ErrorCode: 0,
			Path:      "/kafka",
			Topics: []DescribeLogDirsResponseTopic{
				{
					Topic: "random",
					Partitions: []DescribeLogDirsResponsePartition{
						{
							PartitionID: 25,
							Size:        125,
							OffsetLag:   0,
							IsTemporary: false,
						},
						{
							PartitionID: 26,
							Size:        100,
							OffsetLag:   0,
							IsTemporary: false,
						},
					},
				},
			},
		},
	}
	testVersionDecodable(t, "two partitions", response, describeLogDirsResponseTwoPartitionsV2, 2)
	if len(response.LogDirs) != 1 {
		t.Error("Expected one log dirs")
	}
	if len(response.LogDirs[0].Topics) != 1 {
		t.Error("Expected one topic in log dirs")
	}
	if len(response.LogDirs[0].Topics[0].Partitions) != 2 {
		t.Error("Expected two partitions")
	}
}

func TestDescribeLogDirsResponseV3(t *testing.T) {
	// Test empty response
	response := &DescribeLogDirsResponse{
		LogDirs: []DescribeLogDirsResponseDirMetadata{},
	}
	testVersionDecodable(t, "empty", response, describeLogDirsResponseEmptyV3, 3)
	if len(response.LogDirs) != 0 {
		t.Error("Expected no log dirs")
	}

	response.LogDirs = []DescribeLogDirsResponseDirMetadata{
		{
			ErrorCode: 0,
			Path:      "/kafka",
			Topics: []DescribeLogDirsResponseTopic{
				{
					Topic: "random",
					Partitions: []DescribeLogDirsResponsePartition{
						{
							PartitionID: 25,
							Size:        125,
							OffsetLag:   0,
							IsTemporary: false,
						},
						{
							PartitionID: 26,
							Size:        100,
							OffsetLag:   0,
							IsTemporary: false,
						},
					},
				},
			},
		},
	}
	testVersionDecodable(t, "two partitions", response, describeLogDirsResponseTwoPartitionsV3, 3)
	if len(response.LogDirs) != 1 {
		t.Error("Expected one log dirs")
	}
	if len(response.LogDirs[0].Topics) != 1 {
		t.Error("Expected one topic in log dirs")
	}
	if len(response.LogDirs[0].Topics[0].Partitions) != 2 {
		t.Error("Expected two partitions")
	}
}

func TestDescribeLogDirsResponseV4(t *testing.T) {
	// Test empty response
	response := &DescribeLogDirsResponse{
		LogDirs: []DescribeLogDirsResponseDirMetadata{},
	}
	testVersionDecodable(t, "empty", response, describeLogDirsResponseEmptyV4, 4)
	if len(response.LogDirs) != 0 {
		t.Error("Expected no log dirs")
	}

	response.LogDirs = []DescribeLogDirsResponseDirMetadata{
		{
			ErrorCode: 0,
			Path:      "/kafka",
			Topics: []DescribeLogDirsResponseTopic{
				{
					Topic: "random",
					Partitions: []DescribeLogDirsResponsePartition{
						{
							PartitionID: 25,
							Size:        125,
							OffsetLag:   0,
							IsTemporary: false,
						},
						{
							PartitionID: 26,
							Size:        100,
							OffsetLag:   0,
							IsTemporary: false,
						},
					},
				},
			},
		},
	}
	testVersionDecodable(t, "two partitions", response, describeLogDirsResponseTwoPartitionsV4, 4)
	if len(response.LogDirs) != 1 {
		t.Error("Expected one log dirs")
	}
	if len(response.LogDirs[0].Topics) != 1 {
		t.Error("Expected one topic in log dirs")
	}
	if len(response.LogDirs[0].Topics[0].Partitions) != 2 {
		t.Error("Expected two partitions")
	}
	if response.LogDirs[0].TotalBytes != 274726920192 {
		t.Error("Expected total bytes, 274726920192; found:", response.LogDirs[0].TotalBytes)
	}
	if response.LogDirs[0].UsableBytes != 80486563840 {
		t.Error("Expected total bytes, 80486563840; found:", response.LogDirs[0].UsableBytes)
	}
}
