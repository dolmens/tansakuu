#[derive(Default)]
pub struct RangeFieldEncoder {}

const MASK: u64 = 0xF;

impl RangeFieldEncoder {
    pub fn tokenize(&self, value: u64) -> Vec<u64> {
        let mut value = value;
        let mut tokens = Vec::with_capacity(16);
        tokens.push(value);
        for level in 1..16 {
            value >>= 4;
            tokens.push(value + (level << 60));
        }
        tokens
    }

    pub fn searching_ranges(&self, left: u64, right: u64) -> (Vec<(u64, u64)>, Vec<(u64, u64)>) {
        let mut bottom_ranges = vec![];
        let mut higher_ranges = vec![];

        let mut left = left;
        let mut right = right;

        let mut level = 0;
        loop {
            let level_ranges = if level == 0 {
                &mut bottom_ranges
            } else {
                &mut higher_ranges
            };
            let has_lower = (left & MASK) != 0;
            let has_upper = (right & MASK) != MASK;
            // next_right may be negative
            let next_left = ((left >> 4) as i64) + if has_lower { 1 } else { 0 };
            let next_right = ((right >> 4) as i64) - if has_upper { 1 } else { 0 };

            if level == 15 || next_left > next_right {
                let left_bound = left + (level << 60);
                let right_bound = right + (level << 60);
                level_ranges.push((left_bound, right_bound));
                break;
            }

            if has_lower {
                let left_bound = left + (level << 60);
                let right_bound = (left | MASK) + (level << 60);
                level_ranges.push((left_bound, right_bound));
            }

            if has_upper {
                let left_bound = (right & (!MASK)) + (level << 60);
                let right_bound = right + (level << 60);
                level_ranges.push((left_bound, right_bound));
            }

            left = next_left as u64;
            right = next_right as u64;

            level += 1;
        }

        (bottom_ranges, higher_ranges)
    }
}

#[cfg(test)]
mod tests {
    use crate::index::range::range_field_encoder::MASK;

    use super::RangeFieldEncoder;

    macro_rules! assert_ranges {
        ($start:expr, $end:expr, $expected_bottom:expr, $expected_higher:expr) => {
            let encoder = RangeFieldEncoder::default();
            let (bottom_ranges, higher_ranges) = encoder.searching_ranges($start, $end);
            assert_eq!(bottom_ranges, $expected_bottom);
            assert_eq!(higher_ranges, $expected_higher);
        };
    }
    macro_rules! make_range {
        ($left:expr, $right:expr, $level:expr) => {
            ($left + ($level << 60), $right + ($level << 60))
        };
    }

    #[test]
    fn test_tokenize() {
        let value = u64::MAX;
        let mut token = value;
        let mut expect = vec![token];
        for level in 1..16 {
            token >>= 4;
            expect.push(token + (level << 60));
        }
        assert_eq!(RangeFieldEncoder::default().tokenize(value), expect);
    }

    #[test]
    fn test_searching_basic() {
        assert_ranges!(0, 0, vec![(0, 0)], vec![]);
        assert_ranges!(1, 1, vec![(1, 1)], vec![]);
        assert_ranges!(2, 16, vec![(2, 16)], vec![]);
        assert_ranges!(28, 36, vec![(28, 36)], vec![]);
        assert_ranges!(28, 48, vec![(28, 31), (48, 48)], vec![make_range!(2, 2, 1)]);
        assert_ranges!(32, 47, vec![], vec![make_range!(2, 2, 1)]);
        assert_ranges!(240, 256, vec![(256, 256)], vec![make_range!(15, 15, 1)]);
        assert_ranges!(255, 511, vec![(255, 255)], vec![make_range!(1, 1, 2)]);
        assert_ranges!(0, (11_u64 << 32) - 1, vec![], vec![make_range!(0, 10, 8)]);
        assert_ranges!(
            1_u64 << 32,
            1_u64 << 32 | 30,
            vec![(1_u64 << 32 | 16, 1_u64 << 32 | 30)],
            vec![make_range!(1_u64 << 28, 1_u64 << 28, 1)]
        );

        assert_ranges!(
            (4 << 40) - 1,
            (2 << 44) + (2 << 12) + (1 << 4) + 3,
            vec![
                ((4 << 40) - 1, ((4 << 40) - 1) | MASK),
                (35184372097040_u64, 35184372097043_u64)
            ],
            vec![
                make_range!(2199023256064_u64, 2199023256064_u64, 1),
                make_range!(8589934592_u64, 8589934593_u64, 3),
                make_range!(4, 15, 10),
                make_range!(1, 1, 11),
            ]
        );
    }

    #[test]
    fn test_searching_top_level() {
        assert_ranges!(
            1_u64 << 60,
            (3_u64 << 60) - 1,
            vec![],
            vec![make_range!(1, 2, 15)]
        );
        assert_ranges!(0, u64::MAX, vec![], vec![make_range!(0, 15, 15)]);
        let left = (1_u64 << 60) - 1;
        assert_ranges!(
            left,
            u64::MAX,
            vec![(left, left)],
            vec![make_range!(1, 15, 15)]
        );
        let higher: Vec<_> = (1..16).map(|i| make_range!(1, 15, i)).collect();
        assert_ranges!(1, u64::MAX, vec![(1, 15)], higher);
        assert_ranges!(0, left, vec![], vec![make_range!(0, 0, 15)]);
        assert_ranges!(
            0,
            left + 65536,
            vec![],
            vec![
                make_range!(17592186044416_u64, 17592186044416_u64, 4),
                make_range!(0, 0, 15)
            ]
        );
    }

    #[test]
    fn test_searching_big_number() {
        let left = u64::MAX - 5;
        assert_ranges!(left, u64::MAX, vec![(left, u64::MAX)], vec![]);
        let left = u64::MAX - 15;
        assert_ranges!(left, u64::MAX - 1, vec![(left, u64::MAX - 1)], vec![]);
        assert_ranges!(
            left,
            u64::MAX,
            vec![],
            vec![make_range!(left >> 4, u64::MAX >> 4, 1)]
        );
        let left = u64::MAX - 65536 - 255;
        assert_ranges!(
            left,
            u64::MAX,
            vec![],
            vec![
                make_range!(left >> 8, left >> 8, 2),
                make_range!(u64::MAX >> 16, u64::MAX >> 16, 4)
            ]
        );
        let right = u64::MAX - 4096 * 15;
        let left = u64::MAX - 65536 * 2 - 15;
        let maxseg = (u64::MAX >> 16) - 1;
        assert_ranges!(
            left,
            right,
            vec![],
            vec![
                make_range!(left >> 4, left >> 4, 1),
                make_range!(right >> 12, right >> 12, 3),
                make_range!(maxseg, maxseg, 4)
            ]
        );
    }
}
