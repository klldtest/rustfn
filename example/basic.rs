use rustfn::Bot;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Bot::new("!")
        .on_ready(|bot| async move {
            println!("Bot ready as {}", bot.user.display_name);
        })
        .on_friend_request(|request| async move {
            if request.is_inbound() {
                request.accept().await?;
            }
            Ok(())
        })
        .command("hello", |ctx| async move { ctx.send("Hello!").await })
        .command("ping", |ctx| async move { ctx.send("Pong!").await })
        .run()
        .await?;

    Ok(())
}