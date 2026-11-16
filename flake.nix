# Offline vendored build of the Rust crates from the committed Cargo.lock, the same way the fleet's pkgs/obs-board builds them.
# The `native` check of .ci/ccid.toml builds these, so a stale Cargo.lock fails CI instead of the fleet publish.
{
  description = "cmqt: observability relay and server";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/b7c2ada94fe99c15b0dbcf4d11fd7850b957a436";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = nixpkgs.legacyPackages.${system};
      src = pkgs.lib.cleanSourceWith {
        src = ./.;
        filter = path: type: let b = baseNameOf path; in !(builtins.elem b [ "target" "web" "node_modules" ".git" ".ci" ".forgejo" ]);
      };
      rust = name: extra: pkgs.rustPlatform.buildRustPackage ({
        pname = name;
        version = "0.1.0";
        inherit src;
        cargoLock.lockFile = ./Cargo.lock;
        doCheck = false;
        meta.mainProgram = name;
      } // extra);
      cmqt = rust "cmqt" { cargoBuildFlags = [ "-p" "cmqt-relay" ]; };
      cmqt-server = rust "cmqt-server" { cargoBuildFlags = [ "-p" "cmqt-server" ]; };
    in
    {
      packages.${system} = { inherit cmqt cmqt-server; };
      checks.${system} = { inherit cmqt cmqt-server; };
    };
}
