// Local defaults match docker-compose.yml, so a clean checkout runs without a .env file.
// They are local docker credentials only; any other environment must set both variables.
const DEFAULT_OWNER_URL = "postgres://pipeline_owner:pipeline_owner@localhost:54329/pipeline";
const DEFAULT_APP_URL = "postgres://pipeline_app:pipeline_app@localhost:54329/pipeline";

export const ownerDatabaseUrl = (): string => process.env.OWNER_DATABASE_URL ?? DEFAULT_OWNER_URL;
export const appDatabaseUrl = (): string => process.env.APP_DATABASE_URL ?? DEFAULT_APP_URL;
