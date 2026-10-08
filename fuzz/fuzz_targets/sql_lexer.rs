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

    // Repeated placeholders may render one parameter many times, and each
    // backslash expands to a `chr(92)` expression. Keep this generated value
    // small so the harness exercises those cases without quadratic test
    // allocations for a 16 KiB input.
    const MAX_TEXT_CHARS: usize = 16;
    const MAX_TEXT_BYTES: usize = MAX_TEXT_CHARS * 4;
    let values = [
        NzValue::Null,
        NzValue::Int4(-7),
        NzValue::Text(sql.chars().rev().take(MAX_TEXT_CHARS).collect()),
        NzValue::Text("it's \\ \"q\" ; -- /* $$".into()),
        NzValue::Bool(true),
    ];
    for count in 0..=values.len() {
        let positional = substitute_parameters(sql, &values[..count]);
        assert_eq!(positional, substitute_parameters(sql, &values[..count]));
        // Each placeholder is at least two SQL bytes; escaped test literals
        // fit in this conservative upper bound even when placeholders repeat.
        if let Ok(rendered) = &positional {
            let max_replacements = sql.len() / 2;
            let max_literal_bytes = 16 * MAX_TEXT_BYTES + 32;
            let max_rendered = sql
                .len()
                .saturating_add(max_replacements.saturating_mul(max_literal_bytes));
            assert!(rendered.len() <= max_rendered);
        }
    }

    let mut bound: Vec<NzParameter> = values.iter().cloned().map(NzParameter::positional).collect();
    bound.push(NzParameter::named("a", NzValue::Int4(1)));
    for count in 0..=bound.len() {
        let first = substitute_bound_parameters(sql, &bound[..count]);
        assert_eq!(first, substitute_bound_parameters(sql, &bound[..count]));
    }
});
