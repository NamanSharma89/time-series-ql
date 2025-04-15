use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1},
    character::complete::{alphanumeric1, multispace0, multispace1, char, digit1},
    combinator::{map, opt},
    multi::separated_list0,
    sequence::{delimited, pair, preceded, tuple},
    IResult,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Query {
    pub select: Vec<Aggregation>,
    pub from: String,
    pub window: WindowSpec,
    pub filter: Option<FilterExpr>,
    pub group_by: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Aggregation {
    Avg(String),
    Sum(String),
    Count(String),
    Min(String),
    Max(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowSpec {
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterExpr {
    pub field: String,
    pub op: FilterOp,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FilterOp {
    Eq,
    Gt,
    Lt,
    Gte,
    Lte,
}

pub fn parse_query(input: &str) -> IResult<&str, Query> {
    // Main query parser implementation
    // SELECT avg(temperature) FROM sensors 
    // WHERE location = 'datacenter-1' 
    // WINDOW 5m 
    // GROUP BY device_id
    let (input, _) = tag_no_case("SELECT")(input)?;
    let (input, _) = multispace1(input)?;
    
    // Parse aggregations
    let (input, aggregations) = separated_list0(
        tuple((multispace0, tag(","), multispace0)),
        parse_aggregation,
    )(input)?;

    // Parse FROM clause
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case("FROM")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, table) = alphanumeric1(input)?;

    // Parse optional WHERE clause
    let (input, filter) = opt(parse_where_clause)(input)?;

    // Parse WINDOW clause
    let (input, window) = parse_window_clause(input)?;

    // Parse optional GROUP BY clause
    let (input, group_by) = opt(parse_group_by_clause)(input)?;

    Ok((input, Query {
        select: aggregations,
        from: table.to_string(),
        window,
        filter,
        group_by,
    }))
}

fn parse_aggregation(input: &str) -> IResult<&str, Aggregation> {
    let (input, agg_type) = alt((
        tag_no_case("avg"),
        tag_no_case("sum"),
        tag_no_case("count"),
        tag_no_case("min"),
        tag_no_case("max"),
    ))(input)?;
    
    let (input, field) = delimited(
        char('('),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        char(')'),
    )(input)?;

    let agg = match agg_type.to_lowercase().as_str() {
        "avg" => Aggregation::Avg(field.to_string()),
        "sum" => Aggregation::Sum(field.to_string()),
        "count" => Aggregation::Count(field.to_string()),
        "min" => Aggregation::Min(field.to_string()),
        "max" => Aggregation::Max(field.to_string()),
        _ => unreachable!(),
    };

    Ok((input, agg))
}

fn parse_where_clause(input: &str) -> IResult<&str, FilterExpr> {
    let (input, _) = preceded(multispace1, tag_no_case("WHERE"))(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, field) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let (input, _) = multispace0(input)?;
    
    let (input, op) = alt((
        tag("="),
        tag(">="),
        tag("<="),
        tag(">"),
        tag("<"),
    ))(input)?;
    
    let (input, _) = multispace0(input)?;
    let (input, value) = delimited(
        char('\''),
        take_while1(|c: char| c != '\''),
        char('\''),
    )(input)?;

    let op = match op {
        "=" => FilterOp::Eq,
        ">" => FilterOp::Gt,
        "<" => FilterOp::Lt,
        ">=" => FilterOp::Gte,
        "<=" => FilterOp::Lte,
        _ => unreachable!(),
    };

    Ok((input, FilterExpr {
        field: field.to_string(),
        op,
        value: value.to_string(),
    }))
}

fn parse_window_clause(input: &str) -> IResult<&str, WindowSpec> {
    let (input, _) = preceded(multispace1, tag_no_case("WINDOW"))(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, amount) = digit1(input)?;
    let (input, unit) = take_while1(|c: char| c.is_alphabetic())(input)?;
    
    let amount: u64 = amount.parse().unwrap();
    let duration = match unit {
        "s" => Duration::from_secs(amount),
        "m" => Duration::from_secs(amount * 60),
        "h" => Duration::from_secs(amount * 3600),
        "d" => Duration::from_secs(amount * 86400),
        _ => return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::Tag
        ))),
    };

    Ok((input, WindowSpec { duration }))
}

fn parse_group_by_clause(input: &str) -> IResult<&str, Vec<String>> {
    let (input, _) = preceded(multispace1, tag_no_case("GROUP BY"))(input)?;
    let (input, _) = multispace1(input)?;
    
    let (input, fields) = separated_list0(
        tuple((multispace0, char(','), multispace0)),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
    )(input)?;

    Ok((input, fields.into_iter().map(|s| s.to_string()).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_query() {
        let query = "SELECT avg(temperature) FROM sensors WHERE location = 'datacenter-1' WINDOW 5m GROUP BY device_id";
        let result = parse_query(query);
        assert!(result.is_ok());
    }
}