use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields};

#[proc_macro_derive(Spawn)]
pub fn sub_agent_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let expanded = match &input.data {
        syn::Data::Struct(data) => {
            let fields = &data.fields;
            let field_names = fields.iter().map(|f| &f.ident).collect::<Vec<_>>();
            let field_types = fields.iter().map(|f| &f.ty).collect::<Vec<_>>();

            quote! {
                impl #struct_name {
                    pub fn spawn(
                        cancellation_token: CancellationToken,
                        #(#field_names: #field_types),*
                    ) -> tokio::task::JoinHandle<()> {
                        tokio::spawn(async move {
                            let agent = #struct_name {
                                #(#field_names),*
                            };

                            let res = cancellation_token.run_until_cancelled(agent.run()).await;

                            match res {
                                Some(res) => {
                                    match res {
                                        Ok(_) => {
                                            tracing::info!("{} finished", stringify!(#struct_name));
                                        }
                                        Err(e) => {
                                            tracing::error!("{} finished with Error: {:?}", stringify!(#struct_name), e);
                                        }
                                    };
                                    cancellation_token.cancel();
                                }
                                None => {
                                    tracing::info!("{} cancelled", stringify!(#struct_name));
                                }
                            };
                        })
                    }
                }
            }
        }
        _ => panic!("Spawn can only be derived for structs"),
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(Id)]
pub fn derive_id(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let is_valid = match input.data {
        Data::Struct(ref data) => matches!(
            data.fields,
            Fields::Unnamed(ref fields) if fields.unnamed.len() == 1
        ),
        _ => false,
    };

    if !is_valid {
        return syn::Error::new_spanned(name, "Id derive macro can only be used with tuple structs containing a single field.")
            .to_compile_error()
            .into();
    }
    let expanded = quote! {
        impl From<Digest> for #name {
            fn from(digest: Digest) -> Self {
                Self(digest)
            }
        }
    };
    TokenStream::from(expanded)
}
