package idxadvisor

import (
	"fmt"

    "github.com/pingcap/parser/model"
    "github.com/pingcap/tidb/infoschema"
	plannercore "github.com/pingcap/tidb/planner/core"
)

// IndicesWithCost includes in indices and their physical plan cost.
type IndicesWithCost struct {
	Indices []*IdxAndTblInfo
	Cost    float64
}

// IdxAndTblInfo provides a IndexInfo and its TableInfo.
type IdxAndTblInfo struct {
	Index   *model.IndexInfo
	Table	*model.TableInfo
}

// Deviation is a deviation standard for comparing benefit.
const Deviation = 0.01

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
func SaveVirtualIndices(is infoschema.InfoSchema, dbname string, iwc IndicesWithCost, connectionID uint64, origCost float64) {
    fmt.Printf("***Connection id %d, virtual physical plan's cost: %f, original cost: %f, \n***Virtual index:", connectionID, iwc.Cost, origCost)
    benefit := (origCost - iwc.Cost) / origCost
	if benefit < Deviation {
		fmt.Println("needn't create index")
		return
	}

	if _, ok := registeredIdxAdv[connectionID]; !ok {
		registeredIdxAdv[connectionID] = new(IdxAdvisor)
	}

	indices := iwc.Indices
	ia := registeredIdxAdv[connectionID]
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

// WriteResult prints virtual indices and their benefit.
func WriteResult() {
	fmt.Println("----------------------Result----------------------")
	for _, v := range registeredIdxAdv {
		for _, i := range v.Candidate_idx {
			fmt.Printf("%s: ", i.Index.Index.Table.L)
			fmt.Printf("(")
			for _, col := range i.Index.Index.Columns {
				fmt.Printf("%s ", col.Name.L)
			}
			fmt.Printf("\b)    %f    \n", i.Benefit)
		}
		fmt.Println("-----------------------------------------------")
	}
}