.PHONY: prod-up prod-down dev-up dev-down tests-up tests-down


prod-up:
	@docker compose up --build api nginx

prod-down:
	@docker compose down api nginx

dev-up:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build api nginx

dev-down:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml down api nginx

tests-up:
	@docker compose -f docker-compose.yml -f fastapi-project/tests/functional/docker-compose.yml up --build tests

tests-down:
	@docker compose -f docker-compose.yml -f fastapi-project/tests/functional/docker-compose.yml down tests
