package idxadvisor

import (
	"fmt"
	"os"
	"sort"

	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
)

//TODO: outputPath should be passed in by flag
const outputPath string = "/tmp/indexadvisor/"
const sepString string = "    "
const TopN = 3

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
func SaveVirtualIndices(is infoschema.InfoSchema, dbname string, iwc IndicesWithCost, connID uint64, origCost float64) {
	ia := GetIdxAdv(connID)
	indices := iwc.Indices
	ia.queryCnt++

	idxes := make([]*model.IndexInfo, len(indices), len(indices))
	for i, indice := range indices {
		idxes[i] = indice.Index
	}
	writeResultToFile(connID, ia.queryCnt, origCost, iwc.Cost, idxes)

	fmt.Printf("***Connection id %d, virtual physical plan's cost: %f, original cost: %f \n", connID, iwc.Cost, origCost)
	benefit := origCost - iwc.Cost
	if benefit/origCost < Deviation {
		fmt.Println("needn't create index")
		return
	}

	fmt.Printf("***Index:")
	for _, idx := range indices {
		table, err := is.TableByName(model.NewCIStr(dbname), idx.Table.Name)
		if err != nil {
			panic(err)
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
}

func writeResultToFile(connID uint64, queryCnt uint64, origCost, vcost float64, indices []*model.IndexInfo) {
	origCostPrefix := fmt.Sprintf("%v_OCOST", DBNAME)
	origCostOut := fmt.Sprintf("%-10d%f\n", queryCnt, origCost)
	writeToFile(origCostPrefix, origCostOut, true)

	virtualCostPrefix := fmt.Sprintf("%v_OVCOST", DBNAME)
	virtualCostOut := fmt.Sprintf("%-10d%f\n", queryCnt, vcost)
	writeToFile(virtualCostPrefix, virtualCostOut, true)

	virtualIdxPrefix := fmt.Sprintf("%v_OINDEX", DBNAME)
	virtualIdxOut := fmt.Sprintf("%-10d{%s}\n", queryCnt, buildIdxOutputInfo(indices))
	writeToFile(virtualIdxPrefix, virtualIdxOut, true)

	origSummaryPrefix := fmt.Sprintf("%v_ORIGIN", DBNAME)
	origSummaryOut := fmt.Sprintf("%-10d%f%v%f%v{%v}\n", queryCnt, origCost, sepString, vcost, sepString, buildIdxOutputInfo(indices))
	writeToFile(origSummaryPrefix, origSummaryOut, true)
}

func writeToFile(filename, content string, append bool) {
	fileName := fmt.Sprintf("%s%s", outputPath, filename)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if !append {
		fd, err = os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	}
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	fd.WriteString(content)
}

func buildIdxOutputInfo(indices []*model.IndexInfo) string {
	var vIdxesInfo string
	if len(indices) == 0 {
		return ""
	}
	for _, idx := range indices {
		var singleIdx string = "("
		for _, col := range idx.Columns {
			singleIdx = fmt.Sprintf("%s%s ", singleIdx, col.Name.L)
		}
		singleIdx = fmt.Sprintf("%v) ", singleIdx[:len(singleIdx)-1])
		vIdxesInfo = fmt.Sprintf("%s%s", vIdxesInfo, singleIdx)
	}
	return vIdxesInfo
}

// WriteFinaleResult saves virtual indices and their benefit.
func WriteFinaleResult() {
	for id, v := range registeredIdxAdv {
		sort.Sort(v.Candidate_idx)
		resFile := fmt.Sprintf("%v_RESULT", id)
		content := ""
		for _, i := range v.Candidate_idx {
			content += fmt.Sprintf("%s: (", i.Index.Index.Table.L)
			for _, col := range i.Index.Index.Columns {
				content += fmt.Sprintf("%s,", col.Name.L)
			}
			content = content[:len(content)-1]
			content += fmt.Sprintf(")    %f\n", i.Benefit)
		}
		writeToFile(resFile, content, false)
	}
}
