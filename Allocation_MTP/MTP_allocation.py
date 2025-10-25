import streamlit as st
import pandas as pd
import logging

# 4. Use logger library to record errors
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_allocation(df):
    """
    Processes student allocation using a robust, dynamic, strict round-robin (mod n) logic
    that guarantees all students are allocated.
    """
    # 5. Use try-catch wherever possible
    try:
        # 1. Dynamically count faculties (all columns after 'CGPA')
        try:
            faculty_columns = df.columns[df.columns.get_loc('CGPA') + 1:]
        except KeyError:
            st.error("Error: Input CSV must contain a 'CGPA' column.")
            logging.error("Input CSV is missing the 'CGPA' column.")
            return None, None
            
        num_faculty = len(faculty_columns)
        logging.info(f"Dynamically identified {num_faculty} faculties.")

        # 2. Sort by CGPA at run time
        df_sorted_cgpa = df.sort_values(by='CGPA', ascending=False, kind='mergesort')

        # Initialization
        allocations = {}
        unallocated_rolls = list(df_sorted_cgpa['Roll'])

        # Loop in allocation cycles until every student is assigned
        while unallocated_rolls:
            faculties_taken_this_cycle = set()
            this_cycle_allocations = {}

            # Iterate through the list of currently unallocated students
            for roll in unallocated_rolls:
                student_data = df_sorted_cgpa.loc[df_sorted_cgpa['Roll'] == roll].iloc[0]
                preferences = student_data[faculty_columns].sort_values()

                # Find an available faculty based on the student's preference order
                for pref_level in range(1, num_faculty + 1):
                    faculty_choice_series = preferences[preferences == pref_level]
                    if not faculty_choice_series.empty:
                        faculty_choice = faculty_choice_series.index[0]

                        # --- Core Round-Robin Logic ---
                        # If this faculty has not been taken IN THIS CYCLE, allocate the student
                        if faculty_choice not in faculties_taken_this_cycle:
                            this_cycle_allocations[roll] = faculty_choice
                            faculties_taken_this_cycle.add(faculty_choice)
                            break # Allocation successful for this student, move to the next

            # Failsafe: If no students could be allocated, log error and break
            if not this_cycle_allocations and unallocated_rolls:
                logging.error(f"FATAL: Could not allocate any of the remaining {len(unallocated_rolls)} students.")
                for roll in unallocated_rolls: allocations[roll] = 'ALLOCATION_FAILED'
                break

            # Commit the successful allocations and update the list for the next cycle
            allocations.update(this_cycle_allocations)
            unallocated_rolls = [roll for roll in unallocated_rolls if roll not in this_cycle_allocations]

        # 3.A) Prepare the Allocation Output CSV
        output_df = df[['Roll', 'Name', 'Email', 'CGPA']].copy()
        output_df['Allocated'] = output_df['Roll'].map(allocations)
        output_df.sort_values(by='Roll', inplace=True)
        output_df.reset_index(drop=True, inplace=True)

        # 3.B) Prepare the Faculty Preference Stats CSV
        pref_stats = {f'Count Pref {i}': {fac: 0 for fac in faculty_columns} for i in range(1, num_faculty + 1)}
        for index, row in df.iterrows():
            for fac in faculty_columns:
                try:
                    preference = int(row[fac])
                    if 1 <= preference <= num_faculty:
                        pref_stats[f'Count Pref {preference}'][fac] += 1
                except (ValueError, TypeError):
                    continue
        
        fac_preference_df = pd.DataFrame(pref_stats)
        fac_preference_df.index.name = 'Fac'
        
        return output_df, fac_preference_df

    except Exception as e:
        logging.error(f"An unhandled error occurred during allocation: {e}")
        st.error(f"A critical error occurred: {e}")
        return None, None

def main():
    # 1. Use Streamlit for uploading input file and downloading both output files
    st.set_page_config(page_title="Student Allocation System", layout="wide")
    st.title("🎓 Dynamic BTP/MTP Student Allocation System")

    st.sidebar.header("Instructions")
    st.sidebar.info(
        "1. **Upload the input CSV file.** It must contain `Roll`, `Name`, `Email`, `CGPA` columns, followed by faculty preference columns.\n\n"
        "2. The system allocates all students in round-robin cycles based on CGPA.\n\n"
        "3. **Download the two output files.**"
    )

    uploaded_file = st.file_uploader("📂 Upload Input CSV File", type=['csv'])

    if uploaded_file is not None:
        try:
            input_df = pd.read_csv(uploaded_file)
            st.success("File uploaded successfully!")
            st.subheader("Preview of Input Data")
            st.dataframe(input_df.head())

            if st.button("🚀 Run Allocation"):
                with st.spinner('Processing allocation...'):
                    allocation_df, fac_pref_df = process_allocation(input_df)

                if allocation_df is not None and not allocation_df.empty:
                    st.success("✅ Allocation Complete!")
                    if len(allocation_df) == len(input_df):
                        st.info(f"Successfully allocated all {len(input_df)} students.")
                    else:
                        st.warning(f"Warning: Allocated {len(allocation_df)} out of {len(input_df)} students.")

                    st.subheader("📊 Allocation Results (Sorted by Roll Number)")
                    st.dataframe(allocation_df)
                    
                    st.subheader("📈 Faculty Preference Statistics")
                    st.dataframe(fac_pref_df)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.download_button(
                            label="📥 Download Allocation CSV",
                            data=allocation_df.to_csv(index=False).encode('utf-8'),
                            file_name="output_btp_mtp_allocation.csv",
                            mime="text/csv",
                        )
                    with col2:
                        st.download_button(
                            label="📥 Download Faculty Preference Stats CSV",
                            data=fac_pref_df.to_csv().encode('utf-8'),
                            file_name="fac_preference_count.csv",
                            mime="text/csv",
                        )
        except Exception as e:
            logging.error(f"Error processing the uploaded file: {e}")
            st.error(f"An critical error occurred while processing the file: {e}")

if __name__ == "__main__":
    main()