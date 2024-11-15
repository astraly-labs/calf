use crate::types::Digest;

pub struct Primary {
    channels_from_workers: Vec<tokio::sync::mpsc::Receiver<Digest>>
}


#[cfg(test)]
mod test {

}