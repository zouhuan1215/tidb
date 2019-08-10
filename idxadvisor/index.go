package idxadvisor

import (
	"fmt"
	"os"

	"github.com/pingcap/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
)

const outputPath string = "/tmp/indexadvisor"
const sepString string = "    "

// IndicesWithCost includes in indices and their physical plan cost.
type IndicesWithCost struct {
	Indices []*model.IndexInfo
	Cost    float64
}

// MIN_PRECISION is Precision for comparing cost or benefit.
const MIN_PRECISION = 0.0001

// FindVirtualIndices finds the final physical plan's indices.
func FindVirtualIndices(plan plannercore.PhysicalPlan) []*model.IndexInfo {
	indices := []*model.IndexInfo{}
	travelPhysicalPlan(plan, &indices)
	return indices
}

func travelPhysicalPlan(plan plannercore.PhysicalPlan, indices *[]*model.IndexInfo) {
	if plan == nil {
		return
	}

	switch t := plan.(type) {
	case *plannercore.PhysicalIndexReader:
		for _, idxPlan := range t.IndexPlans {
			switch x := idxPlan.(type) {
			case *plannercore.PhysicalIndexScan:
				*indices = append(*indices, x.Index)
			}
		}
	case *plannercore.PhysicalIndexLookUpReader:
		for _, idxPlan := range t.IndexPlans {
			switch x := idxPlan.(type) {
			case *plannercore.PhysicalIndexScan:
				*indices = append(*indices, x.Index)
			}
		}
	}

	for _, p := range plan.Children() {
		travelPhysicalPlan(p, indices)
	}
}

// WriteResult save virtual indices and cost and print them.
func WriteResult(iwc IndicesWithCost, connID uint64, origCost float64) {
	indices := iwc.Indices
	ia := GetIdxAdv(connID)
	ia.queryCnt++
	WriteResultToFile(connID, ia.queryCnt, origCost, iwc.Cost, indices)

	benefit := origCost - iwc.Cost
	if benefit < MIN_PRECISION {
		return
	}

	fmt.Printf("***Connection id %d, virtual physical plan's cost: %f, original cost: %f, \n***Virtual index:", connID, iwc.Cost, origCost)
	if len(indices) > 0 {
		for _, idx := range indices {
			fmt.Printf("(")
			for _, col := range idx.Columns {
				fmt.Printf("%s ", col.Name.L)
			}
			fmt.Printf("\b) ")

			candidateIdx := &CandidateIdx{Index: idx,
				Benefit: benefit,
			}
			ia.addCandidate(candidateIdx)
		}
	}

	fmt.Println("\n----------------Result----------------")
	for _, v := range registeredIdxAdv {
		for _, i := range v.Candidate_idx {
			fmt.Printf("(")
			for _, col := range i.Index.Columns {
				fmt.Printf("%s ", col.Name.L)
			}
			fmt.Printf("\b)    %f    \n", i.Benefit)
		}
		fmt.Println("--------------------------------------")
	}
}

func WriteResultToFile(connID uint64, queryCnt uint64, origCost, vcost float64, indices []*model.IndexInfo) {
	origCostPrefix := fmt.Sprintf("%v_OCOST", connID)
	origCostOut := fmt.Sprintf("%-10d%v\n", queryCnt, origCost)
	WriteToFile(origCostPrefix, origCostOut)

	virtualCostPrefix := fmt.Sprintf("%v_OVCOST", connID)
	virtualCostOut := fmt.Sprintf("%-10d%v\n", queryCnt, vcost)
	WriteToFile(virtualCostPrefix, virtualCostOut)

	virtualIdxPrefix := fmt.Sprintf("%v_OINDEX", connID)
	virtualIdxOut := fmt.Sprintf("%-10d{%s}\n", queryCnt, BuildIdxOutputInfo(indices))
	WriteToFile(virtualIdxPrefix, virtualIdxOut)

	origSummaryPrefix := fmt.Sprintf("%v_ORIGIN", connID)
	origSummaryOut := fmt.Sprintf("%-10d%v%v%v%v{%v}\n", queryCnt, origCost, sepString, vcost, sepString, BuildIdxOutputInfo(indices))
	WriteToFile(origSummaryPrefix, origSummaryOut)
}

func WriteToFile(filePrefix, content string) {
	fileName := fmt.Sprintf("%s/%s", outputPath, filePrefix)
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer fd.Close()

	fd.WriteString(content)
}

func BuildIdxOutputInfo(indices []*model.IndexInfo) string {
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
