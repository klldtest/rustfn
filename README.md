# rustfn

Async Rust client for Epic Online Services and Fortnite.

## Features

- Device code sign-in and saved DeviceAuth credentials.
- Friends, direct messages, and presence.
- Parties, invitations, party chat, and bot commands.

## Installation

Requires Rust 1.88 or newer. Add [rustfn](https://crates.io/crates/rustfn) to your project:

```sh
cargo add rustfn
```

For the example below, add this to `Cargo.toml`:

```toml
[dependencies]
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Basic usage

Save this as `src/main.rs`:

```rust
use rustfn::commands::Bot;

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
```

Run `cargo run`. On first run, complete Epic's device authorization. The bot saves generated DeviceAuth credentials under `./device_auth/` for later runs; keep that directory private. Send `!hello` or `!ping` to try the commands.

## Inspired by

- [rebootpy](https://github.com/xMistt/rebootpy)
- [fnbr.js](https://github.com/fnbrjs/fnbr.js)

## License

MIT.
