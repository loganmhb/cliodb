
use itertools::*;

use std::cmp::max;
use std::fmt::{self, Debug, Formatter};

pub fn debug_table<A, B, C, D, E, F, G>(name: A,
                                        column_names: B,
                                        column_alignments: D,
                                        rows: E)
                                        -> Box<Debug>
    where A: Into<String>,
          B: IntoIterator<Item = C>,
          C: Into<String>,
          D: IntoIterator<Item = Alignment>,
          E: IntoIterator<Item = F>,
          F: IntoIterator<Item = G>,
          G: Into<String>
{
    let name = name.into();
    let col_names = column_names.into_iter().map(Into::into).collect_vec();
    let col_align = column_alignments.into_iter().collect_vec();

    assert_eq!(col_names.len(), col_align.len());

    let mut col_widths = col_names.iter().map(String::len).collect_vec();
    let rows = rows.into_iter().map(|r| r.into_iter().map(Into::into).collect_vec()).collect_vec();

    for row in rows.iter() {
        assert_eq!(col_widths.len(), row.len());

        for (i, x) in row.iter().enumerate() {
            col_widths[i] = max(col_widths[i], x.len());
        }
    }

    let header = format!("| {} |",
                         col_names.into_iter()
                                  .enumerate()
                                  .map(|(i, s)| format!("{:^1$}", s, col_widths[i]))
                                  .join(" | "));

    let sep = header.chars()
                    .map(|c| {
                        match c {
                            '|' => "+",
                            _ => "-",
                        }
                    })
                    .join("");

    Box::new(TablePrinter {
        name: name,
        header: header,
        sep: sep,
        col_widths: col_widths,
        col_align: col_align,
        rows: rows,
    })
}

#[derive(Clone, Copy)]
pub enum Alignment {
    Left,
    Right,
    Center,
}

struct TablePrinter {
    name: String,
    header: String,
    sep: String,
    col_widths: Vec<usize>,
    col_align: Vec<Alignment>,
    rows: Vec<Vec<String>>,
}

impl TablePrinter {
    fn fmt_row(&self, row: &[String]) -> String {
        format!("| {} |",
                row.iter()
                   .enumerate()
                   .map(|(i, s)| {
                       match self.col_align[i] {
                           Alignment::Left => format!("{:<1$}", s, self.col_widths[i]),
                           Alignment::Center => format!("{:^1$}", s, self.col_widths[i]),
                           Alignment::Right => format!("{:>1$}", s, self.col_widths[i]),
                       }
                   })
                   .join(" | "))
    }
}

impl Debug for TablePrinter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        try!(writeln!(f, "{}:", self.name));
        try!(writeln!(f, "{}", self.sep));
        try!(writeln!(f, "{}", self.header));
        try!(writeln!(f, "{}", self.sep));

        for row in self.rows.iter() {
            try!(writeln!(f, "{}", self.fmt_row(&*row)));
        }

        writeln!(f, "{}", self.sep)
    }
}
