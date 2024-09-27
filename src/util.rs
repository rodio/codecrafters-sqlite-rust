use crate::page::ColumnType;

pub fn read_varint(bytes: &[u8]) -> (i64, usize) {
    let mut trimmed_bytes: Vec<u8> = Vec::new();
    let mut continue_bit = true;
    for byte in bytes {
        if !continue_bit {
            break;
        }
        continue_bit = (byte & 0b1000_0000) == 0b1000_0000;

        let trimmed_byte = byte & 0b0111_1111;
        trimmed_bytes.push(trimmed_byte);
    }

    let mut res = [0; 8];

    let mut carryover_bit = false;
    for (i, byte) in trimmed_bytes.iter().enumerate() {
        let mut byte = *byte;
        if carryover_bit {
            byte |= 0b1000_0000;
        }

        if i != trimmed_bytes.len() - 1 {
            res[8 - trimmed_bytes.len() + i] = byte >> 1;
        } else {
            res[8 - trimmed_bytes.len() + i] = byte;
        }

        carryover_bit = byte & 0b0000_0001 == 1;
    }

    (i64::from_be_bytes(res), trimmed_bytes.len())
}

pub fn get_content_size_type(input: i64) -> (usize, ColumnType) {
    if input == 0 {
        return (0, ColumnType::Null);
    }
    if input == 1 {
        return (1, ColumnType::I8);
    }

    //if input >= 12 && input % 2 == 0 {
    //    return (((input - 12) / 2).try_into().unwrap(), ColumnType::Blob);
    //}

    if input >= 13 && input % 2 == 1 {
        return (((input - 13) / 2).try_into().unwrap(), ColumnType::Str);
    }

    (0, ColumnType::I8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let (result, n) = read_varint(&[0x81, 0x47]);
        assert_eq!(result, 199);
        assert_eq!(n, 2);

        let (result, n) = read_varint(&[0x17]);
        assert_eq!(result, 23);
        assert_eq!(n, 1);
    }
}
