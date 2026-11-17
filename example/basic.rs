use rustfn::commands::Bot;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Bot::new("!")
        .on_ready(|bot| async move {
            println!("Bot ready as {} ({})", bot.user.display_name, bot.user.id);
        })
        .on_friend_request(|req| async move {
            if req.is_inbound() {
                req.accept().await?;
            }
            Ok(())
        })
        .on_message(|ctx| async move {
            println!("Message from {}: {}", ctx.author_name(), ctx.message.content);
            Ok(())
        })
        .command("hello", |ctx| async move { ctx.send("Hello!").await })
        .command("ping", |ctx| async move { ctx.send("Pong!").await })
        .run()
        .await?;

    Ok(())
}
