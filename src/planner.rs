use crate::parser::{Operator, Statement, WhereClause};
use crate::storage::DiskDatabase as Database;

#[derive(Debug, Clone)]
pub enum ScanType {
    FullScan,
    IndexScan { index_name: String, column: String },
    IndexRangeScan { index_name: String, column: String },
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub scan_type: ScanType,
    pub table: String,
    pub estimated_rows: usize,
    pub total_rows: usize,
    pub condition: Option<String>,
    pub order_by: Option<String>,
    pub limit: Option<usize>,
}

pub fn plan(db: &Database, stmt: &Statement) -> Option<QueryPlan> {
    let (table, condition, order_by, limit) = match stmt {
        Statement::Select {
            table,
            columns: _,
            condition,
            order_by,
            limit,
        } => (table.clone(), condition.clone(), order_by.clone(), *limit),
        _ => return None,
    };

    let total_rows = db.get_table_row_count(&table);

    let (scan_type, estimated_rows, condition_str) = match &condition {
        Some(WhereClause::Single(cond)) => {
            let op_str = match cond.operator {
                Operator::Eq => "=",
                Operator::Gt => ">",
                Operator::Lt => "<",
            };
            let cond_str = format!("{} {} {}", cond.column, op_str, value_str(&cond.value));

            // Check for matching index
            if let Some((idx_name, _)) = db.get_index_for_column(&table, &cond.column) {
                // Estimate: range scans return ~50% of rows (pessimistic), equality returns ~1 row
                let est = match cond.operator {
                    Operator::Gt | Operator::Lt => total_rows / 2,
                    _ => 1.min(total_rows),
                };
                let scan = match cond.operator {
                    Operator::Gt | Operator::Lt => ScanType::IndexRangeScan {
                        index_name: idx_name.to_string(),
                        column: cond.column.clone(),
                    },
                    _ => ScanType::IndexScan {
                        index_name: idx_name.to_string(),
                        column: cond.column.clone(),
                    },
                };
                (scan, est, Some(cond_str))
            } else {
                (ScanType::FullScan, total_rows, Some(cond_str))
            }
        }
        Some(WhereClause::And(left, right)) => {
            let left_str = where_clause_to_string(left);
            let right_str = where_clause_to_string(right);
            let cond_str = format!("({} AND {})", left_str, right_str);
            (ScanType::FullScan, total_rows, Some(cond_str))
        }
        Some(WhereClause::Or(left, right)) => {
            let left_str = where_clause_to_string(left);
            let right_str = where_clause_to_string(right);
            let cond_str = format!("({} OR {})", left_str, right_str);
            (ScanType::FullScan, total_rows, Some(cond_str))
        }
        None => (ScanType::FullScan, total_rows, None),
    };

    Some(QueryPlan {
        scan_type,
        table,
        estimated_rows,
        total_rows,
        condition: condition_str,
        order_by: order_by
            .map(|(col, asc)| format!("{} {}", col, if asc { "ASC" } else { "DESC" })),
        limit,
    })
}

fn condition_to_string(cond: &crate::parser::Condition) -> String {
    let op_str = match cond.operator {
        Operator::Eq => "=",
        Operator::Gt => ">",
        Operator::Lt => "<",
    };
    format!("{} {} {}", cond.column, op_str, value_str(&cond.value))
}

fn where_clause_to_string(wc: &crate::parser::WhereClause) -> String {
    match wc {
        crate::parser::WhereClause::Single(cond) => condition_to_string(cond),
        crate::parser::WhereClause::And(left, right) => {
            format!(
                "({} AND {})",
                where_clause_to_string(left),
                where_clause_to_string(right)
            )
        }
        crate::parser::WhereClause::Or(left, right) => {
            format!(
                "({} OR {})",
                where_clause_to_string(left),
                where_clause_to_string(right)
            )
        }
    }
}


fn value_str(v: &crate::parser::Value) -> String {
    match v {
        crate::parser::Value::Integer(n) => n.to_string(),
        crate::parser::Value::Float(f) => format!("{:.4}", f)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
        crate::parser::Value::Boolean(b) => b.to_string(),
        crate::parser::Value::Text(s) => format!("'{}'", s),
    }
}

pub fn format_plan(plan: &QueryPlan) -> String {
    let mut output = String::new();
    output.push_str("┌─────────────────────────────────────────┐\n");
    output.push_str("│  Query Plan                             │\n");
    output.push_str("├─────────────────────────────────────────┤\n");

    let operation = match &plan.scan_type {
        ScanType::FullScan => "FULL_SCAN".to_string(),
        ScanType::IndexScan {
            index_name,
            column: _,
        } => {
            format!("INDEX_SCAN ({})", index_name)
        }
        ScanType::IndexRangeScan {
            index_name,
            column: _,
        } => {
            format!("INDEX_RANGE_SCAN ({})", index_name)
        }
    };
    output.push_str(&format!(
        "│  {:<36} │\n",
        format!("Operation  : {}", operation)
    ));
    output.push_str(&format!(
        "│  {:<36} │\n",
        format!("Table      : {}", plan.table)
    ));

    if let Some(condition) = &plan.condition {
        output.push_str(&format!(
            "│  {:<36} │\n",
            format!("Condition  : {}", condition)
        ));
    }

    if let Some(order_by) = &plan.order_by {
        output.push_str(&format!(
            "│  {:<36} │\n",
            format!("Order By   : {}", order_by)
        ));
    }

    if let Some(limit) = &plan.limit {
        output.push_str(&format!("│  {:<36} │\n", format!("Limit      : {}", limit)));
    }

    output.push_str(&format!(
        "│  {:<36} │\n",
        format!(
            "Est. rows  : ~{} of {}",
            plan.estimated_rows, plan.total_rows
        )
    ));
    output.push_str("└─────────────────────────────────────────┘\n");

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_plan_full_scan() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::new(temp_file.path().to_str().unwrap()).unwrap();
        let stmt = Statement::Select {
            table: "users".to_string(),
            columns: vec!["*".to_string()],
            condition: None,
            order_by: None,
            limit: None,
        };
        let result = plan(&db, &stmt);
        assert!(result.is_some());
    }

    #[test]
    fn test_plan_non_select() {
        let temp_file = NamedTempFile::new().unwrap();
        let db = Database::new(temp_file.path().to_str().unwrap()).unwrap();
        let stmt = Statement::Insert {
            table: "users".to_string(),
            values: vec![],
        };
        let result = plan(&db, &stmt);
        assert!(result.is_none());
    }
}
