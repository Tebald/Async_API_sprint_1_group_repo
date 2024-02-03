.PHONY: prod-up
prod-up:
	@docker compose up --build api nginx

.PHONY: prod-down
prod-down:
	@docker compose down api nginx

.PHONY: dev-up
dev-up:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml up --build api nginx

.PHONY: dev-down
dev-down:
	@docker compose -f docker-compose.yml -f docker-compose.dev.yml down api nginx
