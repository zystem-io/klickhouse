use tokio::io::AsyncReadExt;

use crate::{io::ClickhouseRead, values::Value, Result};

use super::{Deserializer, DeserializerState, Type};

pub struct StringDeserializer;

#[allow(clippy::uninit_vec)]
impl Deserializer for StringDeserializer {
    async fn read<R: ClickhouseRead>(
        type_: &Type,
        reader: &mut R,
        rows: usize,
        state: &mut DeserializerState<'_>,
    ) -> Result<Vec<Value>> {
        match type_ {
            Type::String => reader.read_all_strings(state, rows).await,
            Type::FixedString(len) => {
                let len = *len;
                if len > state.string_buf.capacity() {
                    state.string_buf.reserve(len - state.string_buf.capacity());
                }
                let buf_mut =
                    unsafe { std::slice::from_raw_parts_mut(state.string_buf.as_mut_ptr(), len) };

                let mut out = Vec::with_capacity(rows);
                for _ in 0..rows {
                    reader.read_exact(buf_mut).await?;
                    let first_null = buf_mut.iter().position(|x| *x == 0).unwrap_or(len);
                    let effective_slice = state.intern_slice(&buf_mut[..first_null]);
                    out.push(effective_slice);
                }
                Ok(out)
            }
            _ => unimplemented!(),
        }
    }
}
