use thiserror::Error;

pub const GEO_STEP_MAX: usize = 26; // 52 bits
pub const GEO_LAT_MIN: f64 = -85.05112878;
pub const GEO_LAT_MAX: f64 = 85.05112878;
pub const GEO_LONG_MIN: f64 = -180.0;
pub const GEO_LONG_MAX: f64 = 180.0;

const MERCATOR_MAX: f64 = 20037726.37;
// const MERCATOR_MIN: f64 = -20037726.37;

#[derive(Debug, Error)]
pub enum GeoHashError {
    #[error("step {} was not in range 1..={}", .0, GEO_STEP_MAX)]
    InvalidStep(usize),
    #[error("coord ({}, {}) were not in range ({}, {}), ({}, {})",
            .0,.1,GEO_LONG_MIN, GEO_LONG_MAX, GEO_LAT_MIN,GEO_LAT_MAX)]
    InvalidCoord(f64, f64),
}

const B: [u64; 5] = [
    0x5555555555555555,
    0x3333333333333333,
    0x0F0F0F0F0F0F0F0F,
    0x00FF00FF00FF00FF,
    0x0000FFFF0000FFFF,
];

const S: [u32; 5] = [1, 2, 4, 8, 16];

#[inline]
fn interleave64(xlo: u32, ylo: u32) -> u64 {
    let mut x: u64 = xlo as u64;
    let mut y: u64 = ylo as u64;

    x = (x | (x << S[4])) & B[4];
    y = (y | (y << S[4])) & B[4];

    x = (x | (x << S[3])) & B[3];
    y = (y | (y << S[3])) & B[3];

    x = (x | (x << S[2])) & B[2];
    y = (y | (y << S[2])) & B[2];

    x = (x | (x << S[1])) & B[1];
    y = (y | (y << S[1])) & B[1];

    x = (x | (x << S[0])) & B[0];
    y = (y | (y << S[0])) & B[0];

    x | (y << 1)
}

#[inline]
fn check_step(step: usize) -> Result<(), GeoHashError> {
    match step {
        1..=GEO_STEP_MAX => Ok(()),
        _ => Err(GeoHashError::InvalidStep(step)),
    }
}

pub fn geohash_encode(longitude: f64, latitude: f64, step: usize) -> Result<u64, GeoHashError> {
    check_step(step)?;
    if longitude > GEO_LONG_MAX
        || longitude < GEO_LONG_MIN
        || latitude > GEO_LAT_MAX
        || latitude < GEO_LAT_MIN
    {
        return Err(GeoHashError::InvalidCoord(longitude, latitude));
    }

    let long_offset = (longitude - GEO_LONG_MIN) / (GEO_LONG_MAX - GEO_LONG_MIN);
    let lat_offset = (latitude - GEO_LAT_MIN) / (GEO_LAT_MAX - GEO_LAT_MIN);

    let long_offset = long_offset * (1 << step) as f64;
    let lat_offset = lat_offset * (1 << step) as f64;
    let hash = interleave64(lat_offset as u32, long_offset as u32);

    Ok(hash)
}

pub fn geohash_encode_multi_step(
    longitude: f64,
    latitude: f64,
    step_start: usize,
    step_end: usize,
) -> Result<Vec<u64>, GeoHashError> {
    check_step(step_start)?;
    let mut hash = geohash_encode(longitude, latitude, step_end)?;
    let mut codes = vec![];
    codes.push(hash);
    for _ in step_start..step_end {
        hash >>= 2;
        codes.push(hash);
    }
    codes.reverse();

    Ok(codes)
}

#[inline]
pub fn geohash_embed_step(hash: u64, step: usize) -> Result<u64, GeoHashError> {
    check_step(step)?;
    Ok(((step as u64) << 59) | hash)
}

#[inline]
pub fn geohash_extract_step_and_hash(hash: u64) -> (usize, u64) {
    (
        ((hash & !(u64::MAX >> 5)) >> 59) as usize,
        hash & (u64::MAX >> 5),
    )
}

pub fn geohash_encode_multi_step_embed(
    longitude: f64,
    latitude: f64,
    step_start: usize,
    step_end: usize,
) -> Result<Vec<u64>, GeoHashError> {
    let codes = geohash_encode_multi_step(longitude, latitude, step_start, step_end)?;
    Ok(codes
        .iter()
        .zip(step_start..=step_end)
        .map(|(&h, s)| geohash_embed_step(h, s).unwrap())
        .collect())
}

// TODO: This method get a way too low step
pub fn geohash_estimate_steps_by_radius(range_meters: f64, lat: f64) -> usize {
    if range_meters == 0.0 {
        return GEO_STEP_MAX;
    }

    let mut step = 1;
    let mut range = range_meters;

    while range < MERCATOR_MAX {
        range *= 2.0;
        step += 1;
    }
    step -= 2; //<- Make sure range is included in most of the base cases.

    // Wider range towards the poles... Note: it is possible to do better
    // than this approximation by computing the distance between meridians
    // at this latitude, but this does the trick for now.
    if lat > 66.0 || lat < -66.0 {
        step -= 1;
        if lat > 80.0 || lat < -80.0 {
            step -= 1;
        }
    }

    // Frame to valid range.
    if step < 1 {
        step = 1;
    }
    if step > GEO_STEP_MAX {
        step = GEO_STEP_MAX;
    }

    step
}

fn geohash_move_x(hash: u64, step: usize, d: i8) -> u64 {
    if d == 0 {
        return hash;
    }

    let x = hash & 0xaaaaaaaaaaaaaaaa;
    let y = hash & 0x5555555555555555;

    let zz = 0x5555555555555555 >> (64 - step * 2);

    let mut new_x;
    if d > 0 {
        new_x = x + (zz + 1);
    } else {
        new_x = x | zz;
        new_x = new_x - (zz + 1);
    }
    new_x &= 0xaaaaaaaaaaaaaaaa >> (64 - step * 2);

    new_x | y
}

fn geohash_move_y(hash: u64, step: usize, d: i8) -> u64 {
    if d == 0 {
        return hash;
    }

    let x = hash & 0xaaaaaaaaaaaaaaaa;
    let y = hash & 0x5555555555555555;

    let zz = 0xaaaaaaaaaaaaaaaa >> (64 - step * 2);
    let mut new_y;
    if d > 0 {
        new_y = y + (zz + 1);
    } else {
        new_y = y | zz;
        new_y = new_y - (zz + 1);
    }
    new_y &= 0x5555555555555555 >> (64 - step * 2);

    x | new_y
}

#[derive(Debug)]
pub struct GeoHashNeighbors {
    pub north: u64,
    pub east: u64,
    pub west: u64,
    pub south: u64,
    pub north_east: u64,
    pub south_east: u64,
    pub north_west: u64,
    pub south_west: u64,
}

impl Into<Vec<u64>> for GeoHashNeighbors {
    fn into(self) -> Vec<u64> {
        vec![
            self.north,
            self.east,
            self.west,
            self.south,
            self.north_east,
            self.south_east,
            self.north_west,
            self.south_west,
        ]
    }
}

pub fn geohash_neighbors(hash: u64, step: usize) -> Result<GeoHashNeighbors, GeoHashError> {
    check_step(step)?;

    let east = geohash_move_x(hash, step, 1);
    let west = geohash_move_x(hash, step, -1);
    let south = geohash_move_y(hash, step, -1);
    let north = geohash_move_y(hash, step, 1);

    let north_west = geohash_move_x(hash, step, -1);
    let north_west = geohash_move_y(north_west, step, 1);

    let north_east = geohash_move_x(hash, step, 1);
    let north_east = geohash_move_y(north_east, step, 1);

    let south_east = geohash_move_x(hash, step, 1);
    let south_east = geohash_move_y(south_east, step, -1);

    let south_west = geohash_move_x(hash, step, -1);
    let south_west = geohash_move_y(south_west, step, -1);

    Ok(GeoHashNeighbors {
        north,
        east,
        west,
        south,
        north_east,
        south_east,
        north_west,
        south_west,
    })
}
