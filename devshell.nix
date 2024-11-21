{
  mkShell,
  lib,
  rust-analyzer-unwrapped,
  rustfmt,
  clippy,
  cargo,
  rustc,
  rustPlatform,
  openssl,
  pkg-config,
  python311Packages
}:
mkShell {
  strictDeps = true;

  # Python analytics dependencies
  packages = with python311Packages; [
    python
    elasticsearch
    elastic-transport
    matplotlib
  ];

  nativeBuildInputs = [
    cargo
    rustc

    rust-analyzer-unwrapped
    rustfmt
    clippy
    openssl
    pkg-config
  ];

  buildInputs = [];

  shellHook = ''
    export PKG_CONFIG_PATH="${openssl.dev}/lib/pkgconfig";
  '';

  env = {
    RUST_SRC_PATH = "${rustPlatform.rustLibSrc}";
  };
}
