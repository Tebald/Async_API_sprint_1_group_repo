.PHONY: prod-up prod-down dev-up dev-down tests-up tests-down


prod-up:
	@docker compose up -d --build

prod-down:
	@docker compose down

dev-up:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build

dev-down:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml down

tests-up:
	@docker compose -f docker-compose.tests.yml up -d --build

tests-down:
	@docker compose -f docker-compose.tests.yml down
