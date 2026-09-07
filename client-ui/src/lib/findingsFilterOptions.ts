export type SelectOption = { value: string; label: string };

export const AI_STATUS_OPTIONS: SelectOption[] = [
  { value: "All", label: "All" },
  { value: "has_ai", label: "Has AI Response" },
  { value: "no_ai", label: "No AI Response" },
  { value: "ai_tp", label: "AI TP" },
  { value: "ai_fp", label: "AI FP" },
  { value: "ai_u", label: "AI U" },
];

export const WORK_ITEM_STATUS_OPTIONS: SelectOption[] = [
  { value: "all", label: "All" },
  { value: "any", label: "Has work item" },
  { value: "none", label: "No work item" },
  { value: "OPEN", label: "Open" },
  { value: "IN_PROGRESS", label: "In Progress" },
  { value: "DONE", label: "Done" },
  { value: "CANCELLED", label: "Cancelled / Won't Fix" },
  { value: "UNKNOWN", label: "Unknown" },
];

export const RISK_STATE_LABELS = {
  risk_accepted: "Risk Accepted",
  under_review: "Under Review",
  mitigated: "Mitigated",
} as const;

export function optionLabel(options: SelectOption[], value: string): string {
  return options.find((option) => option.value === value)?.label ?? value;
}
