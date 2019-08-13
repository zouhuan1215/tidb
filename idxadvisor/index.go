package idxadvisor

import (
	"fmt"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
)

// sepString is used to format result
const sepString string = "    "

// IndicesWithCost includes in indices and their physical plan cost.
type IndicesWithCost struct {
	Indices []*IdxAndTblInfo
	Cost    float64
}

// IdxAndTblInfo provides a IndexInfo and its TableInfo.
type IdxAndTblInfo struct {
	Index *model.IndexInfo
	Table *model.TableInfo
}

const (
	// Deviation is a deviation standard for comparing benefit.
	Deviation = 0.01
)

// FindVirtualIndices finds the final physical plan's indices.
func FindVirtualIndices(plan plannercore.PhysicalPlan) []*IdxAndTblInfo {
	indices := []*IdxAndTblInfo{}
	travelPhysicalPlan(plan, &indices)
	return indices
}

func travelPhysicalPlan(plan plannercore.PhysicalPlan, indices *[]*IdxAndTblInfo) {
	if plan == nil {
		return
	}

	switch t := plan.(type) {
	case *plannercore.PhysicalIndexReader:
		for _, idxPlan := range t.IndexPlans {
			switch x := idxPlan.(type) {
			case *plannercore.PhysicalIndexScan:
				x.Index.Table = x.Table.Name
				index := &IdxAndTblInfo{Index: x.Index, Table: x.Table}
				*indices = append(*indices, index)
			}
		}
	case *plannercore.PhysicalIndexLookUpReader:
		for _, idxPlan := range t.IndexPlans {
			switch x := idxPlan.(type) {
			case *plannercore.PhysicalIndexScan:
				x.Index.Table = x.Table.Name
				index := &IdxAndTblInfo{Index: x.Index, Table: x.Table}
				*indices = append(*indices, index)
			}
		}
	}

	for _, p := range plan.Children() {
		travelPhysicalPlan(p, indices)
	}
}

// SaveVirtualIndices saves virtual indices and their benefit.
func SaveVirtualIndices(is infoschema.InfoSchema, dbname string, iwc IndicesWithCost, origCost float64) error {
	ia, err := GetIdxAdv(dbname)
	if err != nil {
		return err
	}
	indices := iwc.Indices
	ia.queryCnt++

	idxes := make([]*model.IndexInfo, len(indices), len(indices))
	for i, indice := range indices {
		idxes[i] = indice.Index
	}

	err = ia.writeResultToFile(ia.queryCnt, origCost, iwc.Cost, idxes)
	if err != nil {
		return err
	}

	fmt.Printf("***Connection id %v, virtual physical plan's cost: %f, original cost: %f \n", dbname, iwc.Cost, origCost)
	benefit := origCost - iwc.Cost
	if benefit/origCost < Deviation {
		fmt.Println("needn't create index")
		return nil
	}

	fmt.Printf("***Index:")
	for _, idx := range indices {
		table, err := is.TableByName(model.NewCIStr(dbname), idx.Table.Name)
		if err != nil {
			return err
		}

		if isExistedInTable(idx.Index, table.Meta().Indices) {
			continue
		}

		candidateIdx := &CandidateIdx{Index: idx,
			Benefit: benefit,
		}
		ia.addCandidate(candidateIdx)

		fmt.Printf(" (")
		tblName := idx.Index.Table.L
		for _, col := range idx.Index.Columns {
			idxCol := tblName + "." + col.Name.L
			fmt.Printf("%s ", idxCol)
		}
		fmt.Printf("\b)\n")
	}

	return nil
}

// GetRecommendIdxStr return recommended index in string format.
func GetRecommendIdxStr(dbname string) (string, error) {
	ia, ok := registeredIdxAdv[dbname]
	if !ok {
		return "", fmt.Errorf("bad attempt to get recommend index with no registered index advisor. connID: %v", dbname)
	}

	idxes := ia.CanIdx
	if len(idxes) == 0 {
		return "", nil
	}

	var idxesStr string
	for _, idx := range idxes {
		var idxStr string
		idxStr = fmt.Sprintf("%s: (", idx.Index.Index.Table.L)
		cols := idx.Index.Index.Columns
		colLen := len(cols)

		for i := 0; i < len(cols)-1; i++ {
			idxStr = fmt.Sprintf("%s%s ", idxStr, cols[i].Name.L)
		}

		idxStr = fmt.Sprintf("%s%s)", idxStr, cols[colLen-1].Name.L)
		idxesStr = fmt.Sprintf("%s%s,", idxesStr, idxStr)
	}
	return idxesStr[:len(idxesStr)-1], nil
}

//// ToString returns indices string.
//func ToString(indices []*model.IndexInfo) string {
//	var vIdxesInfo string
//	if len(indices) == 0 {
//		return ""
//	}
//	for _, idx := range indices {
//		singleIdx := "("
//		for _, col := range idx.Columns {
//			singleIdx = fmt.Sprintf("%s%s ", singleIdx, col.Name.L)
//		}
//		singleIdx = fmt.Sprintf("%v) ", singleIdx[:len(singleIdx)-1])
//		vIdxesInfo = fmt.Sprintf("%s%s", vIdxesInfo, singleIdx)
//	}
//	return vIdxesInfo
//}
