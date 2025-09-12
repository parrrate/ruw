struct Example;

impl ruw::Ruw for Example {
    type State = Vec<u8>;

    type Delta = Vec<u8>;

    type Error = async_std::io::Error;

    type TrackOne = ();

    type TrackMany = ();

    async fn read(&self) -> Result<Self::State, Self::Error> {
        async_std::fs::read("examples/temp.txt").await
    }

    fn update(state: Self::State, delta: Self::Delta) -> Result<Self::State, Self::Error> {
        let _ = state;
        Ok(delta)
    }

    async fn write(&self, old: Self::State, new: Self::State) -> Result<(), Self::Error> {
        let _ = old;
        async_std::task::sleep(std::time::Duration::from_secs(2)).await;
        async_std::fs::write("examples/temp.txt", new).await
    }

    fn accept((): Self::TrackMany) {}

    fn reject((): Self::TrackMany, error: Self::Error) {
        eprintln!("{error}");
    }
}

#[async_std::main]
async fn main() -> Result<(), async_std::io::Error> {
    let (sender, receiver) = async_std::channel::unbounded();
    async_std::task::spawn(ruw::ruw(&Example, receiver));
    let stdin = async_std::io::stdin();
    loop {
        let mut line = String::new();
        stdin.read_line(&mut line).await?;
        let _ = sender.send((line.into(), ())).await;
    }
}
