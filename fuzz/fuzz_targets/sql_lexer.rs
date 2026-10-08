#![no_main]
//! SQL lexer and parameter substitution: no panic, no unbounded growth, and a
//! deterministic result for the same input.
use libfuzzer_sys::fuzz_target;
use nz_rust::messages::{parse_transaction_state, split_statements};
use nz_rust::params::substitute_parameters;
use nz_rust::{substitute_bound_parameters, NzParameter, NzValue};

fuzz_target!(|data: &[u8]| {
    if data.len() > 16_384 {
        return;
    }
    let Ok(sql) = std::str::from_utf8(data) else {
        return;
    };

    let statements = split_statements(sql);
    assert_eq!(statements, split_statements(sql));
    assert_eq!(parse_transaction_state(sql), parse_transaction_state(sql));

    let values = [
        NzValue::Null,
        NzValue::Int4(-7),
        NzValue::Text(sql.chars().rev().collect()),
        NzValue::Text("it's \\ \"q\" ; -- /* $$".into()),
        NzValue::Bool(true),
    ];
    for count in 0..=values.len() {
        let positional = substitute_parameters(sql, &values[..count]);
        assert_eq!(positional, substitute_parameters(sql, &values[..count]));
        // A rendered statement never grows more than the literals can account for.
        if let Ok(rendered) = &positional {
            assert!(rendered.len() <= sql.len() + count * (2 * sql.len() + 64) + 64);
        }
    }

    let mut bound: Vec<NzParameter> = values.iter().cloned().map(NzParameter::positional).collect();
    bound.push(NzParameter::named("a", NzValue::Int4(1)));
    for count in 0..=bound.len() {
        let first = substitute_bound_parameters(sql, &bound[..count]);
        assert_eq!(first, substitute_bound_parameters(sql, &bound[..count]));
    }
});
