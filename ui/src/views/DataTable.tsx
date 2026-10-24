import {
  type ColumnDef,
  columnFilteringFeature,
  type RowData,
  createColumnHelper,
  createFilteredRowModel,
  createPaginatedRowModel,
  createSortedRowModel,
  createTable,
  filterFn_includesString,
  globalFilteringFeature,
  metaHelper,
  rowPaginationFeature,
  rowSortingFeature,
  sortFn_alphanumeric,
  sortFn_basic,
  sortFn_text,
  tableFeatures,
} from "@tanstack/solid-table";
import ArrowDown from "lucide-solid/icons/arrow-down";
import ArrowUp from "lucide-solid/icons/arrow-up";
import ArrowUpDown from "lucide-solid/icons/arrow-up-down";
import ChevronLeft from "lucide-solid/icons/chevron-left";
import ChevronRight from "lucide-solid/icons/chevron-right";
import { createMemo, createSignal, For, type JSX, Match, Show, Switch } from "solid-js";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TextField, TextFieldInput, TextFieldLabel } from "@/components/ui/text-field";
import { cx } from "@/lib/cva";

/** What a column adds to its header and cells: classes, such as `text-right`. */
type Meta = { class?: string };

/** What every table on the page can do: sort, filter its text, and page. */
const features = tableFeatures({
  columnMeta: metaHelper<Meta>(),
  rowSortingFeature,
  columnFilteringFeature,
  globalFilteringFeature,
  rowPaginationFeature,
  filteredRowModel: createFilteredRowModel(),
  sortedRowModel: createSortedRowModel(),
  paginatedRowModel: createPaginatedRowModel(),
  sortFns: { alphanumeric: sortFn_alphanumeric, text: sortFn_text, basic: sortFn_basic },
  filterFns: { includesString: filterFn_includesString },
});

export type Features = typeof features;

/** Denser rows than the table's own, for tables in cards. */
export const dense = "[&_td]:py-1 [&_th]:h-8";

/** The column helper for rows of `T`. */
export const columnsOf = <T extends RowData>() => createColumnHelper<Features, T>();

/**
 * A table of `data`, one row per item, as `columns` say: a column with an
 * accessor sorts by its value (click its header). With `filter`, a box over
 * it filters the rows by their text; with `pageSize`, it shows that many rows
 * a page. `after` adds rows after the table's own, such as a row being
 * edited.
 */
export function DataTable<T extends RowData>(props: {
  data: T[];
  // Each accessor has its own value type.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  columns: ColumnDef<Features, T, any>[];
  rowId: (row: T) => string;
  class?: string;
  /** The filter box's placeholder: no box without it. */
  filter?: string;
  pageSize?: number;
  /** Shown in place of the rows when there are none, or none match. */
  empty?: string;
  rowClass?: (row: T) => string | undefined;
  after?: JSX.Element;
}) {
  // A new array only when rows come or go. A field changing in a row (the
  // data is a reconciled store) updates its cells in place; a new array
  // would make new rows, and lose what their cells hold.
  const data = createMemo(() => [...props.data]);
  const [query, setQuery] = createSignal("");
  // The columns and page size are a table's own: read once.
  // eslint-disable-next-line solid/reactivity
  const columns = props.columns;
  const table = createTable({
    features,
    columns,
    get data() {
      return data();
    },
    getRowId: (row) => props.rowId(row),
    globalFilterFn: "includesString",
    get state() {
      return { globalFilter: query() };
    },
    onGlobalFilterChange: setQuery,
    initialState: {
      // eslint-disable-next-line solid/reactivity
      pagination: { pageIndex: 0, pageSize: props.pageSize ?? Number.MAX_SAFE_INTEGER },
    },
  });
  const pages = () => table.getPageCount();
  return (
    <div class="space-y-2">
      <Show when={props.filter}>
        {(placeholder) => (
          <TextField value={query()} onChange={(value) => table.setGlobalFilter(value)} class="max-w-sm">
            <TextFieldLabel class="sr-only">Filter</TextFieldLabel>
            <TextFieldInput class="h-8" placeholder={placeholder()} />
          </TextField>
        )}
      </Show>
      <Table class={props.class}>
        <TableHeader>
          <For each={table.getHeaderGroups()}>
            {(group) => (
              <TableRow>
                <For each={group.headers}>
                  {(header) => (
                    <TableHead class={header.column.columnDef.meta?.class}>
                      <Show
                        when={header.column.getCanSort()}
                        fallback={<table.FlexRender header={header} />}
                      >
                        <button
                          type="button"
                          class={cx(
                            "hover:text-foreground inline-flex items-center gap-1",
                            header.column.columnDef.meta?.class?.includes("text-right") && "flex-row-reverse",
                          )}
                          onClick={(event) => header.column.getToggleSortingHandler()?.(event)}
                        >
                          <table.FlexRender header={header} />
                          <Switch fallback={<ArrowUpDown class="size-3 opacity-40" />}>
                            <Match when={header.column.getIsSorted() === "asc"}>
                              <ArrowUp class="size-3" />
                            </Match>
                            <Match when={header.column.getIsSorted() === "desc"}>
                              <ArrowDown class="size-3" />
                            </Match>
                          </Switch>
                        </button>
                      </Show>
                    </TableHead>
                  )}
                </For>
              </TableRow>
            )}
          </For>
        </TableHeader>
        <TableBody>
          <For
            each={table.getRowModel().rows}
            fallback={
              <Show when={props.empty && !props.after}>
                <TableRow>
                  <TableCell colSpan={columns.length} class="text-muted-foreground text-center">
                    {props.empty}
                  </TableCell>
                </TableRow>
              </Show>
            }
          >
            {(row) => (
              <TableRow class={props.rowClass?.(row.original)}>
                <For each={row.getAllCells()}>
                  {(cell) => (
                    <TableCell class={cell.column.columnDef.meta?.class}>
                      <table.FlexRender cell={cell} />
                    </TableCell>
                  )}
                </For>
              </TableRow>
            )}
          </For>
          {props.after}
        </TableBody>
      </Table>
      <Show when={pages() > 1}>
        <div class="text-muted-foreground flex items-center justify-end gap-2 text-sm">
          <span>
            Page {table.atoms.pagination.get().pageIndex + 1} of {pages()}
          </span>
          <Button
            size="icon-sm"
            variant="outline"
            aria-label="Previous page"
            disabled={!table.getCanPreviousPage()}
            onClick={() => table.previousPage()}
          >
            <ChevronLeft class="size-4" />
          </Button>
          <Button
            size="icon-sm"
            variant="outline"
            aria-label="Next page"
            disabled={!table.getCanNextPage()}
            onClick={() => table.nextPage()}
          >
            <ChevronRight class="size-4" />
          </Button>
        </div>
      </Show>
    </div>
  );
}
