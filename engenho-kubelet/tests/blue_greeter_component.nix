{ flake ? "github:pleme-io/blue", system ? builtins.currentSystem }:
let
  blue = builtins.getFlake flake;
  src = /. + builtins.unsafeDiscardStringContext "${blue.outPath}/examples/http";
in
(blue.lib.project { inherit src; }).packages.${system}.service-greeter-component
