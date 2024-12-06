#!/bin/bash

cargo fmt --all && cargo clippy --all-targets && cross build --release --target x86_64-unknown-linux-musl && \
cargo deb -p eldegoss_router --target x86_64-unknown-linux-musl --no-build -v
