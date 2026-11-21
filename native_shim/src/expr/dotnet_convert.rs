//! .NET 11 conversions between decimals and floating values, which the engine's casts do not follow:
//! - a decimal as a float or a double is correctly rounded (the engine's cast is off by one ulp for about a quarter of
//!   the values);
//! - a float or a double as a decimal is .NET's System.Decimal of its exact binary value, rounded to the target scale
//!   (the engine's cast multiplies in floating point).

use polars::prelude::*;
use std::cmp::Ordering;
use std::fmt::Write as _;

use crate::types::ExprContext;

const fn powers(base: u128) -> [u128; 39] {
    let mut table = [1u128; 39];
    let mut i = 1;
    while i < table.len() {
        table[i] = table[i - 1] * base;
        i += 1;
    }
    table
}

/// 10^0 ..= 10^38, the range of a 38-digit decimal.
const POW10: [u128; 39] = powers(10);
/// 5^0 ..= 5^28 (System.Decimal's largest scale); 5^28 < 2^66, so 5^s times a double's mantissa fits a u128.
const POW5: [u128; 29] = {
    let all = powers(5);
    let mut table = [0u128; 29];
    let mut i = 0;
    while i < table.len() {
        table[i] = all[i];
        i += 1;
    }
    table
};
/// The powers of ten a double holds exactly (5^22 < 2^53).
const POW10_F64: [f64; 23] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19, 1e20,
    1e21, 1e22,
];
/// The powers of ten a float holds exactly (5^10 < 2^24).
const POW10_F32: [f32; 11] = [1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10];

/// A stack buffer for the text of one decimal value ("<39 digits>e-<2 digits>").
struct DigitBuffer {
    bytes: [u8; 48],
    len: usize,
}

impl std::fmt::Write for DigitBuffer {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        let end = self.len + s.len();
        self.bytes.get_mut(self.len..end).ok_or(std::fmt::Error)?.copy_from_slice(s.as_bytes());
        self.len = end;
        Ok(())
    }
}

/// `digits`·10^-`scale` parsed as `T`, which Rust's float parsing rounds correctly (to `T` directly: no double rounding).
fn parse_scaled<T: std::str::FromStr>(digits: u128, scale: u32) -> T {
    let mut buffer = DigitBuffer { bytes: [0; 48], len: 0 };
    write!(buffer, "{digits}e-{scale}").expect("a 38-digit decimal fits the buffer");
    // SAFETY: only ASCII digits, 'e' and '-' were written.
    let text = unsafe { std::str::from_utf8_unchecked(&buffer.bytes[..buffer.len]) };
    text.parse().ok().expect("decimal digits parse as a float")
}

/// u / 10^scale against k·2^exp, exactly (k < 2^56 and scale ≤ 22, so k·5^scale < 2^108).
fn compare_scaled(u: u128, scale: u32, k: u64, exp: i32) -> Ordering {
    // u / 10^scale vs k·2^exp  ⟺  u vs k·5^scale·2^(exp + scale)
    let a = k as u128 * POW5[scale as usize];
    let shift = exp + scale as i32;
    if shift >= 0 {
        let shift = shift as u32;
        if shift >= a.leading_zeros() { Ordering::Less } else { u.cmp(&(a << shift)) }
    } else {
        let shift = (-shift) as u32;
        if shift >= u.leading_zeros() { Ordering::Greater } else { (u << shift).cmp(&a) }
    }
}

/// u (> 0) as a double within an ulp or two, from two exact u64 conversions.
fn approx_f64(u: u128) -> f64 {
    let high = (u >> 64) as u64;
    if high == 0 { u as u64 as f64 } else { high as f64 * 18446744073709551616.0 + (u as u64) as f64 }
}

/// The nearest value of a binary format to u / 10^scale (u > 0, scale ≤ 22: a normal value), ties to even: a candidate
/// from double arithmetic, checked against its two midpoints exactly and moved by an ulp while it is not the nearest.
/// None when three steps do not settle it. `MANTISSA_BITS` is the format's stored mantissa width, `BIAS` the
/// exponent of its least mantissa bit for a biased exponent of 0 (1075 for a double, 150 for a float).
macro_rules! nearest {
    ($name:ident, $float:ty, $bits:ty, $mantissa_bits:expr, $bias:expr) => {
        fn $name(u: u128, scale: u32) -> Option<$float> {
            let mut candidate = (approx_f64(u) / POW10_F64[scale as usize]) as $float;
            for _ in 0..3 {
                let bits = candidate.to_bits();
                let mantissa = ((bits & ((1 << $mantissa_bits) - 1)) | (1 << $mantissa_bits)) as u64;
                let exponent = ((bits >> $mantissa_bits) as i32 & ((1 << (<$bits>::BITS - 1 - $mantissa_bits)) - 1)) - $bias;
                let (up, down) = (<$float>::from_bits(bits + 1), <$float>::from_bits(bits - 1));
                match compare_scaled(u, scale, 2 * mantissa + 1, exponent - 1) {
                    Ordering::Greater => { candidate = up; continue; }
                    Ordering::Equal => return Some(if mantissa & 1 == 0 { candidate } else { up }),
                    Ordering::Less => {}
                }
                // Below the least mantissa of a binade, the next value down is half as far.
                let (k, e) = if mantissa == 1 << $mantissa_bits { (4 * mantissa - 1, exponent - 2) } else { (2 * mantissa - 1, exponent - 1) };
                match compare_scaled(u, scale, k, e) {
                    Ordering::Less => { candidate = down; continue; }
                    Ordering::Equal => return Some(if mantissa & 1 == 0 { candidate } else { down }),
                    Ordering::Greater => return Some(candidate),
                }
            }
            None
        }
    };
}
nearest!(nearest_f64, f64, u64, 52, 1075);
nearest!(nearest_f32, f32, u32, 23, 150);

/// The decimal `mantissa`·10^-`scale` as the nearest double (ties to even), as .NET 11's (double)decimal: exact
/// operands make one correctly rounded operation (Clinger's fast path); other values below scale 23 are found by
/// exact comparison; the rest are parsed.
fn decimal_to_f64(mantissa: i128, scale: u32) -> f64 {
    let u = mantissa.unsigned_abs();
    let magnitude = if u == 0 {
        0.0
    } else if scale == 0 {
        u as f64
    } else if u < 1 << 53 && scale <= 22 {
        u as f64 / POW10_F64[scale as usize]
    } else if scale <= 22 {
        nearest_f64(u, scale).unwrap_or_else(|| parse_scaled::<f64>(u, scale))
    } else {
        parse_scaled::<f64>(u, scale)
    };
    if mantissa < 0 { -magnitude } else { magnitude }
}

/// The decimal `mantissa`·10^-`scale` as the nearest float (ties to even), rounded once, as .NET 11's (float)decimal.
fn decimal_to_f32(mantissa: i128, scale: u32) -> f32 {
    let u = mantissa.unsigned_abs();
    let magnitude = if u == 0 {
        0.0
    } else if scale == 0 {
        u as f32
    } else if u < 1 << 24 && scale <= 10 {
        u as f32 / POW10_F32[scale as usize]
    } else if scale <= 22 {
        nearest_f32(u, scale).unwrap_or_else(|| parse_scaled::<f32>(u, scale))
    } else {
        parse_scaled::<f32>(u, scale)
    };
    if mantissa < 0 { -magnitude } else { magnitude }
}

/// `value` / 2^`shift`, rounded half to even.
fn shift_right_half_even(value: u128, shift: u32) -> u128 {
    if shift == 0 {
        return value;
    }
    if shift >= 128 {
        // value < 2^127 in every caller, so value / 2^shift < 1/2.
        return 0;
    }
    let quotient = value >> shift;
    let remainder = value & ((1u128 << shift) - 1);
    let half = 1u128 << (shift - 1);
    if remainder > half || (remainder == half && quotient & 1 == 1) { quotient + 1 } else { quotient }
}

/// `value` / `divisor`, rounded half to even.
fn divide_half_even(value: u128, divisor: u128) -> u128 {
    let (quotient, remainder) = (value / divisor, value % divisor);
    let twice = remainder * 2; // remainder < divisor ≤ 10^28: no overflow
    if twice > divisor || (twice == divisor && quotient & 1 == 1) { quotient + 1 } else { quotient }
}

/// |x| = mantissa·2^exponent, for a finite x.
fn binary_parts(x: f64) -> (u64, i32) {
    let bits = x.to_bits();
    let biased = ((bits >> 52) & 0x7FF) as i32;
    let fraction = bits & ((1u64 << 52) - 1);
    if biased == 0 { (fraction, -1074) } else { (fraction | 1u64 << 52, biased - 1075) }
}

/// .NET 11's System.Decimal of a finite double, as its magnitude (mantissa, scale): the exact binary value rounded
/// half to even at the largest scale up to 28 whose mantissa fits 96 bits (29 significant digits while they fit, else
/// 28); None beyond System.Decimal's range.
fn dotnet_decimal_of(x: f64) -> Option<(u128, u32)> {
    let (mantissa, exponent) = binary_parts(x);
    if mantissa == 0 {
        return Some((0, 0));
    }
    // The scale whose mantissa reaches 29 digits is 28 - floor(log10 |x|); this estimate of floor(log10 |x|) is that or
    // one below, so the search starts there or one scale above and steps down.
    let top_bit = 63 - mantissa.leading_zeros() as i32 + exponent;
    let estimate = (top_bit as f64 * std::f64::consts::LOG10_2).floor() as i32;
    let mut scale = (28 - estimate).clamp(0, 28) as u32;
    loop {
        // |x|·10^scale = mantissa·5^scale·2^(exponent + scale); mantissa·5^scale < 2^53·2^66.
        let scaled = mantissa as u128 * POW5[scale as usize];
        let shift = exponent + scale as i32;
        let rounded = if shift >= 0 {
            // A left shift past 96 bits does not fit (a smaller scale may, below).
            if shift < 128 && scaled.leading_zeros() > shift as u32 + 31 { Some(scaled << shift) } else { None }
        } else {
            Some(shift_right_half_even(scaled, (-shift) as u32))
        };
        if let Some(value) = rounded.filter(|&v| v >> 96 == 0) {
            return Some((value, scale));
        }
        if scale == 0 {
            return None;
        }
        scale -= 1;
    }
}

/// floor(d·log2 10) for d = 0..=28: 10^d has that many bits after its leading one.
const LOG2_POW10: [u32; 29] = {
    let mut table = [0u32; 29];
    let mut d = 0;
    while d < table.len() {
        table[d] = 127 - POW10[d].leading_zeros();
        d += 1;
    }
    table
};

/// .NET 11's (decimal)x stored at `scale` (rounded half to even, as Math.Round(value, scale)), as the mantissa of a
/// decimal of `precision` digits; None for NaN, an infinity, or a value beyond System.Decimal or the precision.
fn float_to_decimal(x: f64, precision: u32, scale: u32) -> Option<i128> {
    if !x.is_finite() {
        return None;
    }
    let (digits, from) = dotnet_decimal_of(x)?;
    let stored = if scale >= from {
        digits.checked_mul(POW10[(scale - from) as usize])?
    } else {
        // System.Decimal's value (x rounded at `from` digits) lies within 10^-d / 2 of y = |x|·10^scale, d = from - scale,
        // so rounding it to `scale` rounds y itself, unless y lies that close to a half: only then is it divided.
        let (mantissa, exponent) = binary_parts(x);
        let scaled = mantissa as u128 * POW5[scale as usize]; // y = scaled·2^shift
        let shift = exponent + scale as i32;
        if shift >= 0 {
            scaled << shift // an integer, which System.Decimal holds exactly
        } else if shift <= -128 {
            0 // y < 2^-9: far from a half
        } else {
            let r = (-shift) as u32;
            let (whole, fraction, half) = (scaled >> r, scaled & ((1u128 << r) - 1), 1u128 << (r - 1));
            // 10^-d / 2 is at most 2^(-1 - floor(d·log2 10)): in units of 2^-r, 2^window.
            let window = (r - 1) as i32 - LOG2_POW10[(from - scale) as usize] as i32;
            let distance = fraction.abs_diff(half);
            if distance == 0 || (window >= 0 && distance <= 1u128 << window) {
                divide_half_even(digits, POW10[(from - scale) as usize])
            } else if fraction > half {
                whole + 1
            } else {
                whole
            }
        }
    };
    if stored >= POW10[precision as usize] {
        return None;
    }
    let stored = stored as i128;
    Some(if x.is_sign_negative() { -stored } else { stored })
}

/// A decimal column as `target` (Float32 or Float64), each value correctly rounded; null stays null. A column of any
/// other type takes the engine's cast.
fn dotnet_decimal_to_float(c: Column, target: &DataType) -> PolarsResult<Column> {
    let s = c.as_materialized_series();
    let DataType::Decimal(_, scale) = s.dtype() else {
        return Ok(s.cast(target)?.into_column());
    };
    let scale = *scale as u32;
    let physical = s.decimal()?.physical();
    let out = match target {
        DataType::Float64 => {
            let ca: Float64Chunked = polars_core::prelude::arity::unary_elementwise_values(physical, |m: i128| decimal_to_f64(m, scale));
            ca.into_series()
        }
        DataType::Float32 => {
            let ca: Float32Chunked = polars_core::prelude::arity::unary_elementwise_values(physical, |m: i128| decimal_to_f32(m, scale));
            ca.into_series()
        }
        other => polars_bail!(ComputeError: "a decimal is converted to Float32 or Float64, not {}", other),
    };
    Ok(out.with_name(s.name().clone()).into_column())
}

/// A float or double column as Decimal(`precision`, `scale`), each value .NET 11's (decimal)x rounded to the scale;
/// null for NaN, an infinity or a value beyond the precision (where .NET throws: an engine cannot fail one row only, and
/// a guard the query puts before the conversion, `!double.IsNaN(x) && …`, must keep working); null stays null. A column
/// of any other type takes the engine's cast.
fn dotnet_float_to_decimal(c: Column, precision: usize, scale: usize) -> PolarsResult<Column> {
    let s = c.as_materialized_series();
    if !s.dtype().is_float() {
        return Ok(s.cast(&DataType::Decimal(precision, scale))?.into_column());
    }
    let doubles = s.cast(&DataType::Float64)?; // exact for a float
    let ca: Int128Chunked = polars_core::prelude::arity::unary_elementwise(doubles.f64()?, |v: Option<f64>| {
        v.and_then(|x| float_to_decimal(x, precision as u32, scale as u32))
    });
    Ok(ca.into_decimal_unchecked(precision, scale).into_series().with_name(s.name().clone()).into_column())
}

/// A decimal expression as the floating type of `code` (PlDataTypeKind: Float32 or Float64), each value correctly
/// rounded, as .NET 11's (float)decimal and (double)decimal. Element by element.
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dotnet_decimal_to_float(expr_ptr: *mut ExprContext, code: i32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let target = crate::datatypes::PlDataTypeKind::from_i32(code)
            .map(|kind| kind.to_default_datatype())
            .ok_or_else(|| polars_err!(ComputeError: "invalid DataType code: {}", code))?;
        polars_ensure!(matches!(target, DataType::Float32 | DataType::Float64), ComputeError: "a decimal is converted to Float32 or Float64, not {}", target);
        let output = target.clone();
        let function = move |c: Column| dotnet_decimal_to_float(c, &target);
        let output_map = move |_input_schema: &Schema, input_field: &Field| Ok(Field::new(input_field.name().clone(), output.clone()));
        Ok(Box::into_raw(Box::new(ExprContext { inner: ctx.inner.map(function, output_map) })))
    })
}

/// A float or double expression as Decimal(`precision`, `scale`): .NET 11's (decimal)x of each value, rounded half to
/// even at the scale; null for NaN, an infinity or a value beyond the precision. Element by element.
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dotnet_float_to_decimal(expr_ptr: *mut ExprContext, precision: i32, scale: i32) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        polars_ensure!(
            (1..=38).contains(&precision) && (0..=precision).contains(&scale),
            ComputeError: "invalid decimal precision {} and scale {}", precision, scale
        );
        let (precision, scale) = (precision as usize, scale as usize);
        let function = move |c: Column| dotnet_float_to_decimal(c, precision, scale);
        let output_map = move |_input_schema: &Schema, input_field: &Field| {
            Ok(Field::new(input_field.name().clone(), DataType::Decimal(precision, scale)))
        };
        Ok(Box::into_raw(Box::new(ExprContext { inner: ctx.inner.map(function, output_map) })))
    })
}

/// The culture symbols .NET's ToString() of a float or double writes (NumberFormatInfo).
struct NumberSymbols {
    negative: String,
    positive: String,
    separator: String,
    nan: String,
    positive_infinity: String,
    negative_infinity: String,
}

/// The shortest digits that round-trip a finite value and its decimal exponent (`d.ddd × 10^exponent`), from ryu's
/// text of it (`123.456`, `1.2345e17`, `-0.0`). Ryu takes the nearest of the shortest forms and, of two equally near
/// (`630179542433601.25`), the even one, as .NET does. The digits go to `digits`; zero is "0" at exponent 0.
fn shortest_digits(text: &str, digits: &mut String) -> (bool, i32) {
    let (negative, body) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };
    let (mantissa, exponent) = match body.split_once('e') {
        Some((m, e)) => (m, e.parse::<i32>().expect("an exponent")),
        None => (body, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    digits.clear();
    digits.push_str(whole);
    digits.push_str(fraction);
    // value = digits × 10^(exponent - fraction digits); leading and trailing zeros are not significant.
    let leading = digits.bytes().take_while(|&b| b == b'0').count();
    digits.drain(..leading);
    let trailing = digits.bytes().rev().take_while(|&b| b == b'0').count();
    digits.truncate(digits.len() - trailing);
    if digits.is_empty() {
        digits.push('0');
        return (negative, 0);
    }
    let scaled = exponent - fraction.len() as i32 + trailing as i32;
    (negative, scaled + digits.len() as i32 - 1)
}

/// Writes a finite value as .NET's ToString() does ("R": the shortest digits that round-trip): fixed-point unless the
/// decimal point lies beyond `round_trip_digits` (17 for a double, 9 for a float: .NET's max(digits, round-trip digits),
/// as the shortest form has no more) or more than three places left of the digits, then `d.dddE+XX` (two exponent
/// digits at least). -0.0 is "-0".
fn write_dotnet_shortest(out: &mut String, negative: bool, digits: &str, exponent: i32, round_trip_digits: usize, symbols: &NumberSymbols) {
    let count = digits.len();
    let scale = exponent + 1; // digits before the decimal point
    if negative {
        out.push_str(&symbols.negative);
    }
    if scale > round_trip_digits as i32 || scale < -3 {
        out.push_str(&digits[..1]);
        if count > 1 {
            out.push_str(&symbols.separator);
            out.push_str(&digits[1..]);
        }
        out.push('E');
        out.push_str(if exponent < 0 { &symbols.negative } else { &symbols.positive });
        write!(out, "{:02}", exponent.unsigned_abs()).expect("writing to a String");
    } else if scale <= 0 {
        out.push('0');
        out.push_str(&symbols.separator);
        (0..-scale).for_each(|_| out.push('0'));
        out.push_str(digits);
    } else if (scale as usize) < count {
        out.push_str(&digits[..scale as usize]);
        out.push_str(&symbols.separator);
        out.push_str(&digits[scale as usize..]);
    } else {
        out.push_str(digits);
        (count..scale as usize).for_each(|_| out.push('0'));
    }
}

/// A float or double column as .NET's ToString() writes each value under the culture of `symbols`; null stays null.
fn dotnet_float_to_string(c: Column, symbols: &NumberSymbols) -> PolarsResult<Column> {
    let s = c.as_materialized_series();
    let (mut ryu, mut digits) = (ryu::Buffer::new(), String::new());
    let ca = match s.dtype() {
        DataType::Float64 => s.f64()?.apply_into_string_amortized(|x: f64, out: &mut String| {
            if x.is_nan() {
                out.push_str(&symbols.nan)
            } else if x.is_infinite() {
                out.push_str(if x > 0.0 { &symbols.positive_infinity } else { &symbols.negative_infinity })
            } else {
                let (negative, exponent) = shortest_digits(ryu.format_finite(x), &mut digits);
                write_dotnet_shortest(out, negative, &digits, exponent, 17, symbols)
            }
        }),
        DataType::Float32 => s.f32()?.apply_into_string_amortized(|x: f32, out: &mut String| {
            if x.is_nan() {
                out.push_str(&symbols.nan)
            } else if x.is_infinite() {
                out.push_str(if x > 0.0 { &symbols.positive_infinity } else { &symbols.negative_infinity })
            } else {
                let (negative, exponent) = shortest_digits(ryu.format_finite(x), &mut digits);
                write_dotnet_shortest(out, negative, &digits, exponent, 9, symbols)
            }
        }),
        other => polars_bail!(ComputeError: ".NET float formatting takes Float32 or Float64, not {}", other),
    };
    Ok(ca.into_series().with_name(s.name().clone()).into_column())
}

/// A float or double expression as .NET's ToString() of each value (the shortest round-trip digits, laid out as .NET
/// does) with the culture symbols given (UTF-8): negative and positive signs, decimal separator, NaN, infinities.
#[unsafe(no_mangle)]
pub extern "C" fn pl_expr_dotnet_float_to_string(
    expr_ptr: *mut ExprContext,
    negative: *const std::os::raw::c_char,
    positive: *const std::os::raw::c_char,
    separator: *const std::os::raw::c_char,
    nan: *const std::os::raw::c_char,
    positive_infinity: *const std::os::raw::c_char,
    negative_infinity: *const std::os::raw::c_char,
) -> *mut ExprContext {
    ffi_try!({
        let ctx = unsafe { Box::from_raw(expr_ptr) };
        let text = |p: *const std::os::raw::c_char| {
            crate::utils::ptr_to_str(p).map(str::to_owned).map_err(|e| polars_err!(ComputeError: "a culture symbol is not UTF-8: {}", e))
        };
        let symbols = std::sync::Arc::new(NumberSymbols {
            negative: text(negative)?,
            positive: text(positive)?,
            separator: text(separator)?,
            nan: text(nan)?,
            positive_infinity: text(positive_infinity)?,
            negative_infinity: text(negative_infinity)?,
        });
        let function = move |c: Column| dotnet_float_to_string(c, &symbols);
        let output_map = |_input_schema: &Schema, input_field: &Field| Ok(Field::new(input_field.name().clone(), DataType::String));
        Ok(Box::into_raw(Box::new(ExprContext { inner: ctx.inner.map(function, output_map) })))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    include!("dotnet_convert_cases.rs");

    #[test]
    fn floats_become_dotnet_decimals_rounded_to_the_scale() {
        for &(bits, precision, scale, expected) in FLOAT_TO_DECIMAL {
            let x = f64::from_bits(bits);
            assert_eq!(float_to_decimal(x, precision, scale), expected, "{x:e} as Decimal({precision}, {scale})");
        }
    }

    #[test]
    fn floats_round_where_system_decimal_ends_then_at_the_scale() {
        // 945001529.969741 is 945001529.96974098682403564453125 exactly. System.Decimal keeps 19 fractional digits,
        // 945001529.9697409868240356445, and Math.Round to 18 breaks that tie to even: …644. One rounding of the exact
        // value to 18 digits would give …645.
        assert_eq!(float_to_decimal(945001529.969741, 38, 18), Some(945_001_529_969_740_986_824_035_644));
        // A float is converted from its exact value, as a double.
        assert_eq!(float_to_decimal(0.1f32 as f64, 38, 27), Some(100_000_001_490_116_119_384_765_625));
    }

    #[test]
    fn decimals_become_the_nearest_float_and_double() {
        for &(mantissa, scale, double_bits, float_bits) in DECIMAL_TO_FLOAT {
            assert_eq!(decimal_to_f64(mantissa, scale).to_bits(), double_bits, "{mantissa}e-{scale} as a double");
            assert_eq!(decimal_to_f32(mantissa, scale).to_bits(), float_bits, "{mantissa}e-{scale} as a float");
        }
    }

    #[test]
    fn a_float_is_rounded_once_from_the_decimal() {
        // Just above the midpoint of 1 and the next float: the nearest double is the midpoint itself, which ties to 1.
        assert_eq!(decimal_to_f32(10_000_000_596_046_447_753_906_250_001, 28), 1.000_000_1f32);
        assert_eq!(decimal_to_f32(16_777_217_000_000_000_000_000_001, 18), 16_777_218.0f32);
    }

    fn convert_column(values: Series, expr: *mut ExprContext) -> PolarsResult<Series> {
        let expr = unsafe { Box::from_raw(expr) }.inner;
        let df = DataFrame::new(values.len(), vec![values.into_column()])?;
        let out = df.lazy().select([expr]).collect()?;
        Ok(out.columns()[0].as_materialized_series().clone())
    }

    fn input(expr: Expr) -> *mut ExprContext {
        Box::into_raw(Box::new(ExprContext { inner: expr }))
    }

    #[test]
    fn decimal_to_float_expression_keeps_nulls_and_has_the_target_type() {
        let decimals = Series::new("a".into(), &[Some(0.1f64), None, Some(-2.5)]).cast(&DataType::Decimal(38, 18)).unwrap();
        for (target, expected) in [(DataType::Float64, [Some(0.1f64), None, Some(-2.5)]), (DataType::Float32, [Some(0.1f32 as f64), None, Some(-2.5)])] {
            let code = crate::datatypes::map_dtype_to_kind(&target) as i32;
            let out = convert_column(decimals.clone(), pl_expr_dotnet_decimal_to_float(input(col("a")), code)).unwrap();
            assert_eq!(out.dtype(), &target);
            assert_eq!(out.name().as_str(), "a");
            let got = out.cast(&DataType::Float64).unwrap();
            assert_eq!((0..got.len()).map(|i| got.f64().unwrap().get(i)).collect::<Vec<_>>(), expected);
        }
        // Anything but a decimal takes the engine's cast.
        let code = crate::datatypes::map_dtype_to_kind(&DataType::Float64) as i32;
        let out = convert_column(Series::new("a".into(), &[3i32]), pl_expr_dotnet_decimal_to_float(input(col("a")), code)).unwrap();
        assert_eq!(out.f64().unwrap().get(0), Some(3.0));
    }

    #[test]
    fn float_to_decimal_expression_keeps_nulls_and_gives_null_on_overflow() {
        let out = convert_column(Series::new("a".into(), &[Some(0.1f64), None, Some(-1.5)]), pl_expr_dotnet_float_to_decimal(input(col("a")), 38, 18)).unwrap();
        assert_eq!(out.dtype(), &DataType::Decimal(38, 18));
        assert_eq!(out.name().as_str(), "a");
        assert_eq!((0..out.len()).map(|i| out.decimal().unwrap().physical().get(i)).collect::<Vec<_>>(), [Some(100_000_000_000_000_006), None, Some(-1_500_000_000_000_000_000)]);
        let floats = convert_column(Series::new("a".into(), &[0.1f32]), pl_expr_dotnet_float_to_decimal(input(col("a")), 38, 18)).unwrap();
        assert_eq!(floats.decimal().unwrap().physical().get(0), Some(100_000_001_490_116_119));
        let out = convert_column(Series::new("a".into(), &[1.0, f64::NAN, f64::INFINITY, 1e20]), pl_expr_dotnet_float_to_decimal(input(col("a")), 38, 18)).unwrap();
        assert_eq!((0..out.len()).map(|i| out.decimal().unwrap().physical().get(i)).collect::<Vec<_>>(), [Some(1_000_000_000_000_000_000), None, None, None]);
        // A literal converts as a column does.
        let out = convert_column(Series::new("b".into(), &[1i32]), pl_expr_dotnet_float_to_decimal(input(lit(0.1f64)), 38, 18)).unwrap();
        assert_eq!(out.decimal().unwrap().physical().get(0), Some(100_000_000_000_000_006));
        // An invalid precision or scale is refused.
        assert!(pl_expr_dotnet_float_to_decimal(input(col("a")), 39, 18).is_null());
        assert!(pl_expr_dotnet_float_to_decimal(input(col("a")), 10, 11).is_null());
    }

    mod format_cases {
        include!("dotnet_format_cases.rs");
    }

    fn symbols(separator: &str, infinity: &str) -> NumberSymbols {
        NumberSymbols {
            negative: "-".into(),
            positive: "+".into(),
            separator: separator.into(),
            nan: "NaN".into(),
            positive_infinity: infinity.into(),
            negative_infinity: format!("-{infinity}"),
        }
    }

    fn formatted(values: Series, symbols: &NumberSymbols) -> Vec<Option<String>> {
        let out = dotnet_float_to_string(values.into_column(), symbols).unwrap();
        let out = out.as_materialized_series().str().unwrap().clone();
        (0..out.len()).map(|i| out.get(i).map(str::to_owned)).collect()
    }

    #[test]
    fn doubles_and_floats_are_written_as_dotnet_writes_them() {
        let (invariant, german) = (symbols(".", "Infinity"), symbols(",", "\u{221E}"));
        let doubles: Vec<f64> = format_cases::DOUBLE_TEXT.iter().map(|&(bits, _, _)| f64::from_bits(bits)).collect();
        let floats: Vec<f32> = format_cases::FLOAT_TEXT.iter().map(|&(bits, _, _)| f32::from_bits(bits)).collect();
        for (symbols, pick) in [(&invariant, 0usize), (&german, 1)] {
            let got = formatted(Series::new("a".into(), &doubles), symbols);
            for (i, &(bits, inv, de)) in format_cases::DOUBLE_TEXT.iter().enumerate() {
                assert_eq!(got[i].as_deref(), Some([inv, de][pick]), "double 0x{bits:016X}");
            }
            let got = formatted(Series::new("a".into(), &floats), symbols);
            for (i, &(bits, inv, de)) in format_cases::FLOAT_TEXT.iter().enumerate() {
                assert_eq!(got[i].as_deref(), Some([inv, de][pick]), "float 0x{bits:08X}");
            }
        }
        let specials = formatted(Series::new("a".into(), &[Some(f64::NAN), Some(f64::INFINITY), Some(f64::NEG_INFINITY), None, Some(-0.0)]), &german);
        assert_eq!(specials, [Some("NaN".into()), Some("\u{221E}".into()), Some("-\u{221E}".into()), None, Some("-0".into())]);
    }
}
