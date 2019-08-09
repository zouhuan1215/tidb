package idxadvisor

import (
    "fmt"

    "github.com/pingcap/parser/model"
	plannercore "github.com/pingcap/tidb/planner/core"
)

// IndicesWithCost includes in indices and their physical plan cost.
type IndicesWithCost struct {
    Indices []*model.IndexInfo
    Cost    float64
}

// Deviation is a deviation standard for comparing benefit.
const Deviation  = 0.01

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
                x.Index.Table = x.Table.Name
                *indices = append(*indices, x.Index)
            }
        }
    case *plannercore.PhysicalIndexLookUpReader:
        for _, idxPlan := range t.IndexPlans {
            switch x := idxPlan.(type) {
            case *plannercore.PhysicalIndexScan:
                x.Index.Table = x.Table.Name
                *indices = append(*indices, x.Index)
            }
		}
	}

    for _, p := range plan.Children() {
        travelPhysicalPlan(p, indices)
    }
}

// WriteResult save virtual indices and cost and print them.
func WriteResult(iwc IndicesWithCost, connectionID uint64, origCost float64) {
    fmt.Printf("***Connection id %d, virtual physical plan's cost: %f, original cost: %f, \n***Virtual index:", connectionID, iwc.Cost, origCost)
    benefit := origCost - iwc.Cost
    if benefit / origCost < Deviation {
        fmt.Println("needn't create index")
        return
    }

    if _, ok := registeredIdxAdv[connectionID]; !ok {
        registeredIdxAdv[connectionID] = new(IdxAdvisor)
    }

    indices := iwc.Indices
    ia := registeredIdxAdv[connectionID]
    if len(indices) != 0 {
        for _, idx := range indices {
            fmt.Printf("(")
            tblName := idx.Table.L
            for _, col := range idx.Columns {
                idxCol := tblName + "." + col.Name.L
                fmt.Printf("%s ", idxCol)
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
            fmt.Printf("%s: ", i.Index.Table.L)
            fmt.Printf("(")
            for _, col := range i.Index.Columns {
                fmt.Printf("%s ", col.Name.L)
            }
            fmt.Printf("\b)    %f    \n", i.Benefit)
        }
        fmt.Println("--------------------------------------")
    }
}