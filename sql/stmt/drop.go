package stmt

type DropTable struct {
	Tables []TableName
}

func (stmt *DropTable) String() string {
	s := "DROP TABLE "
	for i, tbl := range stmt.Tables {
		if i > 0 {
			s += ", "
		}
		s += tbl.String()
	}
	return s
}

func (stmt *DropTable) Dispatch(e Executer) (interface{}, error) {
	return e.DropTable(stmt)
}
