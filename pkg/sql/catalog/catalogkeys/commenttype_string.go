// Code generated by "stringer"; DO NOT EDIT.

package catalogkeys

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[DatabaseCommentType-0]
	_ = x[TableCommentType-1]
	_ = x[ColumnCommentType-2]
	_ = x[IndexCommentType-3]
	_ = x[SchemaCommentType-4]
	_ = x[ConstraintCommentType-5]
	_ = x[FunctionCommentType-6]
}

const _CommentType_name = "DatabaseCommentTypeTableCommentTypeColumnCommentTypeIndexCommentTypeSchemaCommentTypeConstraintCommentTypeFunctionCommentType"

var _CommentType_index = [...]uint8{0, 19, 35, 52, 68, 85, 106, 125}

func (i CommentType) String() string {
	if i < 0 || i >= CommentType(len(_CommentType_index)-1) {
		return "CommentType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _CommentType_name[_CommentType_index[i]:_CommentType_index[i+1]]
}
