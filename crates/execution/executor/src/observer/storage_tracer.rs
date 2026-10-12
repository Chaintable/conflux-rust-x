use impl_tools::autoimpl;
use impl_trait_for_tuples::impl_for_tuples;

#[impl_for_tuples(5)]
#[autoimpl(for<T: trait + ?Sized> &mut T)]
pub trait StorageTracer {}
