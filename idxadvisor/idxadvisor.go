package idxadvisor

import (
    "fmt"
    "context"

    "github.com/pingcap/parser/ast"
    "github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/planner/cascades"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
)

// CandidateIdx includes in index and its benefit.
type CandidateIdx struct {
    Index   *model.IndexInfo
    Benefit float64
}

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
    case *plannercore.PhysicalTableScan:

    }

    for _, p := range plan.Children() {
        travelPhysicalPlan(p, indices)
    }
}

// WriteResult print virtual indices and cost.
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
            fmt.Printf("\b)\n")

            candidateIdx := &CandidateIdx{Index: idx, 
                Benefit: benefit,
            }
            ia.addCandidate(candidateIdx)
        }
    }

    fmt.Println("----------------Result----------------")
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

// OptimizeAndGetLogicPlan executes Optimize and returns a logic plan.
func OptimizeAndGetLogicPlan(ctx context.Context, sctx sessionctx.Context, node ast.Node, is infoschema.InfoSchema) (plannercore.Plan, plannercore.LogicalPlan, error) {
	fp := plannercore.TryFastPlan(sctx, node)
	if fp != nil {
		return fp, nil, nil
	}

	// build logical plan
	sctx.GetSessionVars().PlanID = 0
	sctx.GetSessionVars().PlanColumnID = 0
	builder := plannercore.NewPlanBuilder(sctx, is)
	p, err := builder.Build(ctx, node)
	if err != nil {
		return nil, nil, err
	}

	sctx.GetSessionVars().StmtCtx.Tables = builder.GetDBTableInfo()
	activeRoles := sctx.GetSessionVars().ActiveRoles
	// Check privilege. Maybe it's better to move this to the Preprocess, but
	// we need the table information to check privilege, which is collected
	// into the visitInfo in the logical plan builder.
	if pm := privilege.GetPrivilegeManager(sctx); pm != nil {
		if err := plannercore.CheckPrivilege(activeRoles, pm, builder.GetVisitInfo()); err != nil {
			return nil, nil, err
		}
	}

	if err := plannercore.CheckTableLock(sctx, is, builder.GetVisitInfo()); err != nil {
		return nil, nil, err
	}

	// Handle the execute statement.
	if execPlan, ok := p.(*plannercore.Execute); ok {
		err := execPlan.OptimizePreparedPlan(ctx, sctx, is)
		return p, nil, err
	}

	// Handle the non-logical plan statement.
	logic, isLogicalPlan := p.(plannercore.LogicalPlan)
	if !isLogicalPlan {
		return p, nil, nil
	}

	// Handle the logical plan statement, use cascades planner if enabled.
	if sctx.GetSessionVars().EnableCascadesPlanner {
		plan, err := cascades.FindBestPlan(sctx, logic)
		return plan, nil, err
	}
	plan, err := plannercore.DoOptimize(ctx, builder.GetOptFlag(), logic)
	return plan, logic, err
}