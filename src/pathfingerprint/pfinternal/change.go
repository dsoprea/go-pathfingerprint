package pfinternal

var UpdateTypeCreate string = "create"
var UpdateTypeUpdate string = "update"
var UpdateTypeDelete string = "delete"

var EntityTypeFile string = "file"
var EntityTypePath string = "path"

type ChangeEvent struct {
    EntityType *string
    ChangeType *string
    RelPath *string
}
