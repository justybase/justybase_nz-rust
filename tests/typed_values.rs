use nz_rust::{Decimal, FromSql, NzValue};

#[test]
fn decimal_extraction_preserves_scale_and_rejects_out_of_range_numeric() {
    let exact = Decimal::from_sql(&NzValue::Numeric("3.1400".into())).unwrap();
    assert_eq!(exact.to_string(), "3.1400");
    assert_eq!(Option::<Decimal>::from_sql(&NzValue::Null).unwrap(), None);
    assert!(Decimal::from_sql(&NzValue::Numeric(
        "123456789012345678901234567890123456789".into()
    ))
    .is_err());
}

#[cfg(feature = "chrono")]
#[test]
fn chrono_extraction_keeps_microseconds_and_timezone_offset() {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use nz_rust::NzTimeTz;

    let date = NaiveDate::from_sql(&NzValue::Date("2024-02-29".into())).unwrap();
    assert_eq!(date.to_string(), "2024-02-29");
    let time = NaiveTime::from_sql(&NzValue::Time("23:59:58.123456".into())).unwrap();
    assert_eq!(time.format("%H:%M:%S%.6f").to_string(), "23:59:58.123456");
    let timestamp =
        NaiveDateTime::from_sql(&NzValue::Timestamp("2024-02-29 23:59:58.123456".into())).unwrap();
    assert_eq!(
        timestamp.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        "2024-02-29 23:59:58.123456"
    );
    let timetz = NzTimeTz::from_sql(&NzValue::Timetz("12:34:56.123456-05:30".into())).unwrap();
    assert_eq!(
        timetz.time.format("%H:%M:%S%.6f").to_string(),
        "12:34:56.123456"
    );
    assert_eq!(timetz.offset.local_minus_utc(), -(5 * 3600 + 30 * 60));
    assert!(NzTimeTz::from_sql(&NzValue::Timetz("12:34:56+25:00".into())).is_err());
}
