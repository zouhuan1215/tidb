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
func WriteResult(iwc IndicesWithCost, connectionID uint64, origCost float64) {
    benefit := origCost - iwc.Cost
    if benefit < MIN_PRECISION {
        return
    }

    if _, ok := registeredIdxAdv[connectionID]; !ok {
        registeredIdxAdv[connectionID] = new(IdxAdvisor)
    }

    indices := iwc.Indices
    ia := registeredIdxAdv[connectionID]
    fmt.Printf("***Connection id %d, virtual physical plan's cost: %f, original cost: %f, \n***Virtual index:", connectionID, iwc.Cost, origCost)
    if len(indices) != 0 {
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