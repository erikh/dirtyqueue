auto-test:
	@reflex -d none -R '^(target|.git)/' -r '^(Makefile|.*\.(rs|toml))$$' -- make test

test: 
	@cargo test -- --nocapture

.PHONY: auto-test test
