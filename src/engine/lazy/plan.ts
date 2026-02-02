import type { Expr } from '../../expr/expr';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyExpr = Expr<any>;

export interface ScanNode {
  readonly type: 'scan';
  readonly id: number;
  readonly projection?: string[] | undefined;
}

export interface FilterNode {
  readonly type: 'filter';
  readonly input: PlanNode;
  readonly predicate: AnyExpr;
}

export interface SelectNode {
  readonly type: 'select';
  readonly input: PlanNode;
  readonly columns: string[];
}

export interface ProjectNode {
  readonly type: 'project';
  readonly input: PlanNode;
  readonly exprs: AnyExpr[];
}

export interface GroupByNode {
  readonly type: 'groupby';
  readonly input: PlanNode;
  readonly keys: string[];
  readonly aggs: AnyExpr[];
}

export interface JoinNode {
  readonly type: 'join';
  readonly left: PlanNode;
  readonly right: PlanNode;
  readonly on: string | string[];
  readonly how: string;
}

export interface SortNode {
  readonly type: 'sort';
  readonly input: PlanNode;
  readonly by: string;
  readonly descending: boolean;
}

export interface LimitNode {
  readonly type: 'limit';
  readonly input: PlanNode;
  readonly n: number;
}

export interface DistinctNode {
  readonly type: 'distinct';
  readonly input: PlanNode;
  readonly subset?: string[] | undefined;
}

export type PlanNode =
  | ScanNode
  | FilterNode
  | SelectNode
  | ProjectNode
  | GroupByNode
  | JoinNode
  | SortNode
  | LimitNode
  | DistinctNode;

let nextNodeId = 0;

export function createScanNode(): ScanNode {
  return { type: 'scan', id: nextNodeId++ };
}

export function explainPlan(node: PlanNode, indent = 0): string {
  const pad = '  '.repeat(indent);
  switch (node.type) {
    case 'scan':
      if (node.projection) {
        return `${pad}SCAN [id=${node.id}, cols=${node.projection.join(', ')}]`;
      }
      return `${pad}SCAN [id=${node.id}]`;
    case 'filter':
      return `${pad}FILTER [${node.predicate.toString()}]\n${explainPlan(node.input, indent + 1)}`;
    case 'select':
      return `${pad}SELECT [${node.columns.join(', ')}]\n${explainPlan(node.input, indent + 1)}`;
    case 'project': {
      const exprStrs = node.exprs.map(e => e.toString()).join(', ');
      return `${pad}PROJECT [${exprStrs}]\n${explainPlan(node.input, indent + 1)}`;
    }
    case 'groupby': {
      const aggStrs = node.aggs.map(a => a.toString()).join(', ');
      return `${pad}GROUPBY [keys: ${node.keys.join(', ')}; aggs: ${aggStrs}]\n${explainPlan(node.input, indent + 1)}`;
    }
    case 'join':
      return `${pad}JOIN [${node.how}]\n${pad}  left:\n${explainPlan(node.left, indent + 2)}\n${pad}  right:\n${explainPlan(node.right, indent + 2)}`;
    case 'sort':
      return `${pad}SORT [${node.by} ${node.descending ? 'DESC' : 'ASC'}]\n${explainPlan(node.input, indent + 1)}`;
    case 'limit':
      return `${pad}LIMIT [${node.n}]\n${explainPlan(node.input, indent + 1)}`;
    case 'distinct':
      return `${pad}DISTINCT${node.subset ? ` [${node.subset.join(', ')}]` : ''}\n${explainPlan(node.input, indent + 1)}`;
  }
}
