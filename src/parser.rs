use crate::lexer::Token;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select {
        table: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    },
    Insert {
        table: String,
        values: Vec<Value>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnDef>,
    },
    Delete {
        table: String,
        condition: Option<Condition>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    Gt,
    Lt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    Text(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int,
    Text,
}

pub fn parse(tokens: Vec<Token>) -> Result<Statement, String> {
    let mut tokens = tokens.into_iter().peekable();

    match tokens.next() {
        Some(Token::Select) => parse_select(&mut tokens),
        Some(Token::Insert) => parse_insert(&mut tokens),
        Some(Token::Create) => parse_create_table(&mut tokens),
        Some(Token::Delete) => parse_delete(&mut tokens),
        Some(tok) => Err(format!("Unrecognized statement start: {:?}", tok)),
        None => Err("Empty input".to_string()),
    }
}

fn parse_select(
    tokens: &mut std::iter::Peekable<std::vec::IntoIter<Token>>,
) -> Result<Statement, String> {
    let columns = match tokens.next() {
        Some(Token::Star) => vec!["*".to_string()],
        Some(Token::Ident(name)) => vec![name],
        Some(tok) => return Err(format!("Expected column or *, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    match tokens.next() {
        Some(Token::From) => {}
        Some(tok) => return Err(format!("Expected FROM, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let table = match tokens.next() {
        Some(Token::Ident(name)) => name,
        Some(tok) => return Err(format!("Expected table name, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    let condition = if let Some(&Token::Where) = tokens.peek() {
        tokens.next();
        Some(parse_condition(tokens)?)
    } else {
        None
    };

    match tokens.next() {
        Some(Token::Eof) | None => {}
        Some(tok) => return Err(format!("Unexpected token after statement: {:?}", tok)),
    }

    Ok(Statement::Select {
        table,
        columns,
        condition,
    })
}

fn parse_insert(
    tokens: &mut std::iter::Peekable<std::vec::IntoIter<Token>>,
) -> Result<Statement, String> {
    match tokens.next() {
        Some(Token::Into) => {}
        Some(tok) => return Err(format!("Expected INTO, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let table = match tokens.next() {
        Some(Token::Ident(name)) => name,
        Some(tok) => return Err(format!("Expected table name, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    match tokens.next() {
        Some(Token::Values) => {}
        Some(tok) => return Err(format!("Expected VALUES, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    match tokens.next() {
        Some(Token::LParen) => {}
        Some(tok) => return Err(format!("Expected (, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let mut values = Vec::new();
    loop {
        match tokens.next() {
            Some(Token::Number(n)) => values.push(Value::Integer(n)),
            Some(Token::String(s)) => values.push(Value::Text(s)),
            Some(Token::RParen) => break,
            Some(tok) => return Err(format!("Expected value or ), got: {:?}", tok)),
            None => return Err("Unexpected end of input".to_string()),
        }

        match tokens.next() {
            Some(Token::Comma) => {}
            Some(Token::RParen) => break,
            Some(tok) => return Err(format!("Expected , or ), got: {:?}", tok)),
            None => return Err("Unexpected end of input".to_string()),
        }
    }

    match tokens.next() {
        Some(Token::Eof) | None => {}
        Some(tok) => return Err(format!("Unexpected token after statement: {:?}", tok)),
    }

    Ok(Statement::Insert { table, values })
}

fn parse_create_table(
    tokens: &mut std::iter::Peekable<std::vec::IntoIter<Token>>,
) -> Result<Statement, String> {
    match tokens.next() {
        Some(Token::Table) => {}
        Some(tok) => return Err(format!("Expected TABLE, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let table = match tokens.next() {
        Some(Token::Ident(name)) => name,
        Some(tok) => return Err(format!("Expected table name, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    match tokens.next() {
        Some(Token::LParen) => {}
        Some(tok) => return Err(format!("Expected (, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let mut columns = Vec::new();
    loop {
        let name = match tokens.next() {
            Some(Token::Ident(n)) => n,
            Some(Token::RParen) => break,
            Some(tok) => return Err(format!("Expected column name or ), got: {:?}", tok)),
            None => return Err("Unexpected end of input".to_string()),
        };

        if name == ")" {
            break;
        }

        let data_type = match tokens.next() {
            Some(Token::Ident(dt)) => match dt.to_lowercase().as_str() {
                "int" => DataType::Int,
                "text" => DataType::Text,
                _ => return Err(format!("Unknown data type: {}", dt)),
            },
            Some(tok) => return Err(format!("Expected data type, got: {:?}", tok)),
            None => return Err("Unexpected end of input".to_string()),
        };

        columns.push(ColumnDef { name, data_type });

        match tokens.next() {
            Some(Token::Comma) => continue,
            Some(Token::RParen) => break,
            Some(tok) => return Err(format!("Expected , or ), got: {:?}", tok)),
            None => return Err("Unexpected end of input".to_string()),
        }
    }

    match tokens.next() {
        Some(Token::Eof) | None => {}
        Some(tok) => return Err(format!("Unexpected token after statement: {:?}", tok)),
    }

    Ok(Statement::CreateTable { table, columns })
}

fn parse_delete(
    tokens: &mut std::iter::Peekable<std::vec::IntoIter<Token>>,
) -> Result<Statement, String> {
    match tokens.next() {
        Some(Token::From) => {}
        Some(tok) => return Err(format!("Expected FROM, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    }

    let table = match tokens.next() {
        Some(Token::Ident(name)) => name,
        Some(tok) => return Err(format!("Expected table name, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    let condition = if let Some(&Token::Where) = tokens.peek() {
        tokens.next();
        Some(parse_condition(tokens)?)
    } else {
        None
    };

    match tokens.next() {
        Some(Token::Eof) | None => {}
        Some(tok) => return Err(format!("Unexpected token after statement: {:?}", tok)),
    }

    Ok(Statement::Delete { table, condition })
}

fn parse_condition(
    tokens: &mut std::iter::Peekable<std::vec::IntoIter<Token>>,
) -> Result<Condition, String> {
    let column = match tokens.next() {
        Some(Token::Ident(name)) => name,
        Some(tok) => return Err(format!("Expected column name, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    let operator = match tokens.next() {
        Some(Token::Equals) => Operator::Eq,
        Some(Token::GreaterThan) => Operator::Gt,
        Some(Token::LessThan) => Operator::Lt,
        Some(tok) => return Err(format!("Expected operator, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    let value = match tokens.next() {
        Some(Token::Number(n)) => Value::Integer(n),
        Some(Token::String(s)) => Value::Text(s),
        Some(tok) => return Err(format!("Expected value, got: {:?}", tok)),
        None => return Err("Unexpected end of input".to_string()),
    };

    Ok(Condition {
        column,
        operator,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::tokenize;

    #[test]
    fn test_select_star_from() {
        let tokens = tokenize("SELECT * FROM users");
        let result = parse(tokens);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Statement::Select {
                table: "users".to_string(),
                columns: vec!["*".to_string()],
                condition: None,
            }
        );
    }

    #[test]
    fn test_insert_into_values() {
        let tokens = tokenize("INSERT INTO users VALUES (1, 'sujal')");
        let result = parse(tokens);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Statement::Insert {
                table: "users".to_string(),
                values: vec![Value::Integer(1), Value::Text("sujal".to_string())],
            }
        );
    }

    #[test]
    fn test_create_table() {
        let tokens = tokenize("CREATE TABLE users (id INT, name TEXT)");
        let result = parse(tokens);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Statement::CreateTable {
                table: "users".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Int
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Text
                    },
                ],
            }
        );
    }

    #[test]
    fn test_select_where() {
        let tokens = tokenize("SELECT * FROM users WHERE age > 18");
        let result = parse(tokens);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            Statement::Select {
                table: "users".to_string(),
                columns: vec!["*".to_string()],
                condition: Some(Condition {
                    column: "age".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(18),
                }),
            }
        );
    }
}
