package sql

import (
	"time"
)

const (
	groupMetadataTableName = "group_metadata"
	taskStateTableName     = "task_state"
	taskUUIDSplitSep       = "; "
)

type GroupMetadata struct {
	ID             uint      `gorm:"primary_key"`
	GroupUUID      string    `gorm:"type:varchar(255);uniqueIndex"`
	TaskUUIDS      string    `gorm:"type:text"`
	ChordTriggered bool      `gorm:"type:bool"`
	CreatedAt      time.Time
}

func (GroupMetadata) TableName() string {
	return groupMetadataTableName
}

type TaskState struct {
	ID        uint      `gorm:"primary_key"`
	UUID      string    `gorm:"type:varchar(255);uniqueIndex"`
	Name      string    `gorm:"type:varchar(255)"`
	State     string    `gorm:"type:varchar(255)"`
	Results   string    `gorm:"type:text"`
	Error     string    `gorm:"type:text"`
	CreatedAt time.Time
}

func (TaskState) TableName() string {
	return taskStateTableName
}
