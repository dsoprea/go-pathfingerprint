package pfinternal

import (
    "errors"
    "fmt"
)

const (
    UpdateTypeError_ = iota

    UpdateTypeCreate = iota
    UpdateTypeUpdate = iota
    UpdateTypeDelete = iota
)

const (
    PathStateError_ = iota

    PathStateNew = iota
    PathStateUpdated = iota
    PathStateUnaffected = iota
)

const (
    EntityTypeFile = iota
    EntityTypePath = iota
)

type ChangeEvent struct {
    EntityType int
    ChangeType int
    RelPath string
}

func UpdateTypeName(updateType int) string {
    switch updateType {
    case UpdateTypeCreate:
        return "create"

    case UpdateTypeUpdate:
        return "update"

    case UpdateTypeDelete:
        return "delete"

    default:
        panic(errors.New(fmt.Sprintf("Update-type not valid: (%d)", updateType)))
    }
}

func PathStateName(pathState int) string {
    switch pathState {
    case PathStateNew:
        return "new"

    case PathStateUpdated:
        return "updated"

    case PathStateUnaffected:
        return "unaffected"

    default:
        panic(errors.New(fmt.Sprintf("Path-state not valid: (%d)", pathState)))
    }
}

func EntityTypeName(entityType int) string {
    switch entityType {
    case EntityTypePath:
        return "path"

    case EntityTypeFile:
        return "file"

    default:
        panic(errors.New(fmt.Sprintf("Entity-type not valid: (%d)", entityType)))
    }
}
