use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Select,
    Insert,
    Into,
    Create,
    Table,
    Delete,
    Update,
    Set,
    From,
    Where,
    And,
    Or,
    Values,
    Index,
    Drop,
    On,
    Explain,
    Order,
    By,
    Limit,
    Asc,
    Desc,
    Star,
    Comma,
    LParen,
    RParen,
    Equals,
    GreaterThan,
    LessThan,
    Ident(String),
    Number(i64),
    Float(f64),
    Boolean(bool),
    String(String),
    Eof,
}

pub fn tokenize(input: &str) -> Vec<Token> {
    let mut chars = input.chars().peekable();
    let mut tokens = Vec::new();

    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }

        if c.is_ascii_digit() {
            let (num, is_float) = parse_number(&mut chars);
            if is_float {
                tokens.push(Token::Float(num));
            } else {
                tokens.push(Token::Number(num as i64));
            }
            continue;
        }

        if c == '\'' {
            let s = parse_string(&mut chars);
            tokens.push(Token::String(s));
            continue;
        }

        if c.is_alphabetic() || c == '_' {
            let ident = parse_ident(&mut chars);
            let kw = match ident.to_lowercase().as_str() {
                "select" => Token::Select,
                "insert" => Token::Insert,
                "into" => Token::Into,
                "create" => Token::Create,
                "table" => Token::Table,
                "delete" => Token::Delete,
                "update" => Token::Update,
                "set" => Token::Set,
                "from" => Token::From,
                "where" => Token::Where,
                "and" => Token::And,
                "or" => Token::Or,
                "values" => Token::Values,
                "index" => Token::Index,
                "drop" => Token::Drop,
                "on" => Token::On,
                "explain" => Token::Explain,
                "order" => Token::Order,
                "by" => Token::By,
                "limit" => Token::Limit,
                "asc" => Token::Asc,
                "desc" => Token::Desc,
                "float" => Token::Ident(ident),
                "boolean" => Token::Ident(ident),
                "true" => Token::Boolean(true),
                "false" => Token::Boolean(false),
                _ => Token::Ident(ident),
            };
            tokens.push(kw);
            continue;
        }

        let tok = match c {
            '*' => Token::Star,
            ',' => Token::Comma,
            '(' => Token::LParen,
            ')' => Token::RParen,
            '=' => Token::Equals,
            '>' => Token::GreaterThan,
            '<' => Token::LessThan,
            _ => {
                chars.next();
                continue;
            }
        };
        chars.next();
        tokens.push(tok);
    }

    tokens.push(Token::Eof);
    tokens
}

fn parse_number(chars: &mut Peekable<Chars>) -> (f64, bool) {
    let mut num = 0f64;
    let mut has_dot = false;
    let mut decimals = 0;

    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            let digit = c as u8 - b'0';
            num = num * 10.0 + (digit as f64);
            if has_dot {
                decimals += 1;
            }
            chars.next();
        } else if c == '.' && !has_dot {
            has_dot = true;
            chars.next();
        } else {
            break;
        }
    }

    if has_dot {
        for _ in 0..decimals {
            num /= 10.0;
        }
    }

    (num, has_dot)
}

fn parse_string(chars: &mut Peekable<Chars>) -> String {
    chars.next();
    let mut s = String::new();
    while let Some(&c) = chars.peek() {
        if c == '\'' {
            chars.next();
            break;
        }
        s.push(c);
        chars.next();
    }
    s
}

fn parse_ident(chars: &mut Peekable<Chars>) -> String {
    let mut ident = String::new();
    while let Some(&c) = chars.peek() {
        if c.is_alphanumeric() || c == '_' {
            ident.push(c);
            chars.next();
        } else {
            break;
        }
    }
    ident
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star_from() {
        let tokens = tokenize("SELECT * FROM users");
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Ident("users".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_insert_into_values() {
        let tokens = tokenize("INSERT INTO users VALUES (1, 'sujal')");
        assert_eq!(
            tokens,
            vec![
                Token::Insert,
                Token::Into,
                Token::Ident("users".to_string()),
                Token::Values,
                Token::LParen,
                Token::Number(1),
                Token::Comma,
                Token::String("sujal".to_string()),
                Token::RParen,
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_update_set() {
        let tokens = tokenize("UPDATE users SET name = 'alex'");
        assert_eq!(
            tokens,
            vec![
                Token::Update,
                Token::Ident("users".to_string()),
                Token::Set,
                Token::Ident("name".to_string()),
                Token::Equals,
                Token::String("alex".to_string()),
                Token::Eof,
            ]
        );
    }
}
