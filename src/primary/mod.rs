use crate::types::Digest;

pub struct Primary {
    channels_from_workers: Vec<tokio::sync::mpsc::Receiver<Digest>>
}

impl Primary {
    
}


#[cfg(test)]
mod test {

}