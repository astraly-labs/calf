pub struct Digest {

}

#[derive(Clone, Copy)]
pub struct Transaction {

}

impl Transaction {
    pub fn as_bytes(&self) -> Vec<u8>{
        todo!()
    }


}

pub struct BlockHeader {
    // parents_hash: Vec<Hash>,
}

pub struct Block {
    header: BlockHeader,
}