-- Runs automatically on first container start (postgres image convention):
-- creates a second database for the test suite so dev data is never
-- touched by `pytest`.
CREATE DATABASE handoff_test OWNER handoff;
