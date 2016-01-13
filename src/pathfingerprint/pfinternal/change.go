package pfinternal

var UpdateTypeCreate string = "create"
var UpdateTypeUpdate string = "update"
var UpdateTypeDelete string = "delete"

var PathStateNew string = "path_new"
var PathStateUpdated string = "path_updated"
var PathStateUnaffected string = "path_unaffected"

var EntityTypeFile string = "file"
var EntityTypePath string = "path"

type ChangeEvent struct {
    EntityType *string
    ChangeType *string
    RelPath *string
}
