use cfx_parity_trace_types::SetAuth;
use impl_tools::autoimpl;
use impl_trait_for_tuples::impl_for_tuples;

#[impl_for_tuples(5)]
#[autoimpl(for<T: trait + ?Sized> &mut T)]
#[allow(unused_variables)]
pub trait SetAuthTracer {
    /// Prepares call trace for given params.
    fn record_set_auth(&mut self, set_auth_action: SetAuth) {}
}
