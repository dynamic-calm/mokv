compile:
	protoc *.proto \
		--go_out=. \
		--go_opt=paths=source_relative \
		--proto_path=.
