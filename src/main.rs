mod common;
mod phrase;
mod vocab;

use phrase::write_phrase_filter;
use vocab::write_vocab_fst;

fn main() {
    println!("Generating vocab");
    write_vocab_fst();
    println!("Generating phrase fst");
    write_phrase_filter();
}
