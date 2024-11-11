use core::panic;

use crate::page::ColumnType;

pub fn read_varint(bytes: &[u8]) -> (i64, u8) {
    if bytes.len() > 9 {
        panic!("len of varint is > 9");
    }

    let mut trimmed_bytes: Vec<u8> = Vec::new();
    let mut continue_bit = true;
    for (i, byte) in bytes.iter().enumerate() {
        if !continue_bit {
            break;
        }
        continue_bit = (byte & 0b1000_0000) == 0b1000_0000;

        if i == 8 {
            trimmed_bytes.push(*byte);
            break;
        }

        let trimmed_byte = byte & 0b0111_1111;
        trimmed_bytes.push(trimmed_byte);
    }

    let mut res = 0_i64;
    for (i, byte) in trimmed_bytes.iter().enumerate() {
        if i == 8 {
            res <<= 8;
            res |= *byte as i64;
            break;
        }

        res <<= 7;
        res |= *byte as i64;
    }

    (res, trimmed_bytes.len().try_into().unwrap())
}

pub fn get_content_size_type(input: i64) -> (u64, ColumnType) {
    if input == 0 {
        return (0, ColumnType::Null);
    }

    if input == 1 {
        return (1, ColumnType::I8);
    }

    if input == 2 {
        return (2, ColumnType::I16);
    }

    if input == 3 {
        return (3, ColumnType::I24);
    }

    if input == 8 {
        return (0, ColumnType::Zero);
    }

    if input == 9 {
        return (0, ColumnType::One);
    }

    //if input >= 12 && input % 2 == 0 {
    //    return (((input - 12) / 2).try_into().unwrap(), ColumnType::Blob);
    //}

    if input >= 13 && input % 2 == 1 {
        return (((input - 13) / 2).try_into().unwrap(), ColumnType::Str);
    }

    todo!("column type {input}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let (result, n) = read_varint(&[0x17]);
        assert_eq!(result, 0x17);
        assert_eq!(n, 1);

        let (result, n) = read_varint(&[0x81, 0x47]);
        assert_eq!(result, 199);
        assert_eq!(n, 2);

        let bytes = [
            0b1000_0010,
            0b1110_0001,
            0b1110_0111,
            0b0111_0000,
            0b0000_1011,
            0,
            29,
            37,
            0,
        ];

        let (result, n) = read_varint(&bytes);
        assert_eq!(result, 5796848);
        assert_eq!(n, 4);

        let bytes = [
            0b1000_0010,
            0b1110_0001,
            0b1110_0111,
            0b1111_0000,
            0b1000_1011,
            0b1110_0001,
            0b1110_0111,
            0b1111_0000,
            0b0000_1011,
        ];

        let (result, n) = read_varint(&bytes);
        assert_eq!(result, 398356367593959435);
        assert_eq!(n, 9);
    }
}
