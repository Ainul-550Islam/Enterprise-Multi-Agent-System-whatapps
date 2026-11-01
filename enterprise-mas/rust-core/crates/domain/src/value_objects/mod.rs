//! Domain value objects: validated, immutable-by-convention primitives shared
//! by aggregates.

mod email;
mod resource_limits;
mod semver;
mod slug;
mod token_budget;
mod url;

pub use email::Email;
pub use resource_limits::ResourceLimits;
pub use semver::SemanticVersion;
pub use slug::Slug;
pub use token_budget::TokenBudget;
pub use url::SafeUrl;
