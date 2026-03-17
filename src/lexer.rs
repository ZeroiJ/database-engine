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
    From,
    Where,
    Values,
    Star,
    Comma,
    LParen,
    RParen,
    Equals,
    GreaterThan,
    LessThan,
    Ident(String),
    Number(i64),
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
            let num = parse_number(&mut chars);
            tokens.push(Token::Number(num));
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
                "from" => Token::From,
                "where" => Token::Where,
                "values" => Token::Values,
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

fn parse_number(chars: &mut Peekable<Chars>) -> i64 {
    let mut num = 0i64;
    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            num = num * 10 + (c as i64 - '0' as i64);
            chars.next();
        } else {
            break;
        }
    }
    num
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
}
