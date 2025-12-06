use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, parse_macro_input};

/// Deriving will add concrete implementations for select traits that are necessary for a struct
/// derived in this way to be used as a DirtyQueue generic.
#[proc_macro_derive(DirtyQueue)]
pub fn derive(input: TokenStream) -> TokenStream {
	let input = parse_macro_input!(input as DeriveInput);
	let name = input.ident;

	let expanded = quote! {
		impl crate::IO for #name {}
	};

	TokenStream::from(expanded)
}
