.PHONY: install run test

install:
	@pyenv install 3.13.3 -s
	@pyenv local 3.13.3
	@pipenv install --dev


run:
	@pipenv run python process_data.py

test:
	@pipenv run pytest