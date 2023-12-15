use ractor::{async_trait, Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use tracing::{error, info};

pub enum MyFirstActorMessage {
    PrintHelloWorld,
    HowManyHelloWorlds(RpcReplyPort<u16>),
}

pub struct MyFirstActor;

#[async_trait]
impl Actor for MyFirstActor {
    type State = u16;
    type Msg = MyFirstActorMessage;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _arguments: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(0)
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            MyFirstActorMessage::PrintHelloWorld => {
                info!("Hello world!");
                *state += 1;
            }
            MyFirstActorMessage::HowManyHelloWorlds(reply) => {
                if reply.send(*state).is_err() {
                    error!("Listener dropped their port before we could reply");
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    common_x::log::init_log_filter("debug");

    let (actor, actor_handle) = Actor::spawn(Some("actor".to_string()), MyFirstActor, ())
        .await
        .expect("Actor failed to start");

    for _i in 0..10 {
        // Sends a message, with no reply
        actor
            .cast(MyFirstActorMessage::PrintHelloWorld)
            .expect("Failed to send message to actor");
    }

    let hello_world_count =
        ractor::call_t!(actor, MyFirstActorMessage::HowManyHelloWorlds, 100).expect("RPC failed");

    info!("Actor replied with {} hello worlds!", hello_world_count);

    // Cleanup
    actor.stop(None);
    actor_handle.await.unwrap();
}
