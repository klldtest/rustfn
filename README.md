# rustfn

<img width="1754" height="835" alt="image" src="https://github.com/user-attachments/assets/33c45c63-77a7-46c7-b932-88e96a536489" />

Async Rust client SDK and social/party manager for Epic Online Services and Fortnite.

This project is experimental. Its automated tests use local service mocks; login, chat, and party behavior still need verification against live Epic services.

API naming and behavior follow [fnbr.js](https://github.com/fnbrjs/fnbr.js), which is available under the MIT license.

## Modules

The `rustfn` package contains `protocol`, `http`, `auth`, `xmpp`, `stomp`, `users`, `friends`, `chat`, `party`, `events`, `client`, and `commands`. They are modules of one crate and are available through `rustfn::`.

## Examples

Single bot ([example/basic.rs](example/basic.rs)):

```powershell
cargo run --example basic
```

On first launch without saved credentials under `./device_auth/*.json`, `basic` opens Epic's device authorization prompt in your browser, stores the generated `DeviceAuth` credentials to `./device_auth/<account_id>.json`, accepts incoming friend requests, and registers `!hello` and `!ping`.

```rust
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

```

## Checks

```powershell
cargo check --all-targets
cargo test --all-targets
```
